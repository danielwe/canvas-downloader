use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use lazy_regex::regex;
use reqwest::header;
use unicode_normalization::UnicodeNormalization;

use crate::api::get_canvas_api;
use crate::api::get_pages;
use crate::canvas::{File, FileResult, FolderResult, ProcessOptions};
use crate::utils::{create_folder_if_not_exist_or_ignored, ignored, sanitize_filename_with_id};

pub async fn atomic_download_file(file: File, options: Arc<ProcessOptions>) -> Result<()> {
    // Create tmp file from hash
    let mut tmp_path = file.filepath.clone();
    tmp_path.pop();
    let mut h = DefaultHasher::new();
    file.display_name.hash(&mut h);
    tmp_path.push(h.finish().to_string().add(".tmp"));

    // Aborted download?
    if let Err(e) = download_file((&tmp_path, &file), options.clone()).await {
        if let Err(e) = std::fs::remove_file(&tmp_path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::error!(
                "Failed to remove temporary file {tmp_path:?} for {}, err={e:?}",
                file.display_name
            );
        }
        return Err(e);
    }

    // Update file time
    let updated_at = DateTime::parse_from_rfc3339(&file.updated_at)?;
    let updated_time = filetime::FileTime::from_unix_time(
        updated_at.timestamp(),
        updated_at.timestamp_subsec_nanos(),
    );
    if let Err(e) = filetime::set_file_mtime(&tmp_path, updated_time) {
        tracing::error!(
            "Failed to set modified time of {} with updated_at of {}, err={e:?}",
            file.display_name,
            file.updated_at
        )
    }

    // Atomically rename file, doesn't change mtime
    std::fs::rename(&tmp_path, &file.filepath)?;
    Ok(())
}

async fn download_file(
    (tmp_path, canvas_file): (&Path, &File),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    // Get file
    let mut resp = options
        .client
        .get(&canvas_file.url)
        .bearer_auth(&options.canvas_token)
        .send()
        .await
        .with_context(|| format!("Something went wrong when reaching {}", canvas_file.url))?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "Failed to download {} from {}: HTTP {}",
            canvas_file.display_name,
            canvas_file.url,
            resp.status()
        );
    }

    // Create + Open file
    let mut file = std::fs::File::create(tmp_path)
        .with_context(|| format!("Unable to create tmp file for {:?}", canvas_file.filepath))?;

    // Progress bar
    let download_size = resp
        .headers() // Gives us the HeaderMap
        .get(header::CONTENT_LENGTH) // Gives us an Option containing the HeaderValue
        .and_then(|ct_len| ct_len.to_str().ok()) // Unwraps the Option as &str
        .and_then(|ct_len| ct_len.parse().ok()) // Parses the Option as u64
        .unwrap_or(0); // Fallback to 0
    let progress_bar = options
        .progress_bars
        .add(indicatif::ProgressBar::new(download_size));
    progress_bar.set_message(canvas_file.display_name.to_string());
    progress_bar.set_style(options.progress_style.clone());

    // Download
    while let Some(chunk) = resp.chunk().await? {
        progress_bar.inc(chunk.len() as u64);
        let mut cursor = std::io::Cursor::new(chunk);
        std::io::copy(&mut cursor, &mut file)
            .with_context(|| format!("Could not write to file {:?}", canvas_file.filepath))?;
    }

    progress_bar.finish();
    Ok(())
}

// async recursion needs boxing
pub async fn process_folders(
    (url, path): (String, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    let pages = get_pages(url, &options).await?;

    // For each page
    for pg in pages {
        let uri = pg.url;
        let folders_result = serde_json::from_str::<FolderResult>(&pg.body);

        match folders_result {
            // Got folders
            Ok(FolderResult::Ok(folders)) => {
                for folder in folders {
                    // println!("  * {} - {}", folder.id, folder.name);
                    // The root is already represented by the top-level `files` directory.
                    let folder_path = if folder.parent_folder_id.is_some() {
                        path.join(output_folder_name(folder.id, &folder.name))
                    } else {
                        path.clone()
                    };

                    match create_folder_if_not_exist_or_ignored(&folder_path, &options) {
                        Ok(false) => continue, // ignored
                        Ok(true) => {}         // created or already exists
                        Err(e) => {
                            tracing::error!("{e:#}");
                            continue;
                        }
                    }

                    fork!(
                        process_files,
                        (folder.files_url, folder_path.clone()),
                        (String, PathBuf),
                        options.clone()
                    );
                    fork!(
                        process_folders,
                        (folder.folders_url, folder_path),
                        (String, PathBuf),
                        options.clone()
                    );
                }
            }

            // Got status code
            Ok(FolderResult::Err { status }) => {
                let course_has_no_folders = status == "unauthorized";
                if !course_has_no_folders {
                    tracing::error!(
                        "Failed to access folders at link:{uri}, path:{path:?}, status:{status}",
                    );
                }
            }

            // Parse error
            Err(e) => {
                tracing::error!("Error when getting folders at link:{uri}, path:{path:?}\n{e:?}",);
            }
        }
    }

    Ok(())
}

pub async fn process_files(
    (url, path): (String, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    let pages = get_pages(url, &options).await?;

    // For each page
    for pg in pages {
        let uri = pg.url;
        let files_result = serde_json::from_str::<FileResult>(&pg.body);

        match files_result {
            // Got files
            Ok(FileResult::Ok(files)) => {
                let mut filtered_files = filter_files(&options, &path, files);
                let mut lock = options.files_to_download.lock().await;
                lock.append(&mut filtered_files);
            }

            // Got status code
            Ok(FileResult::Err { status }) => {
                let course_has_no_files = status == "unauthorized";
                if !course_has_no_files {
                    tracing::error!(
                        "Failed to access files at link:{uri}, path:{path:?}, status:{status}",
                    );
                }
            }

            // Parse error
            Err(e) => {
                tracing::error!("Error when getting files at link:{uri}, path:{path:?}\n{e:?}",);
            }
        };
    }

    Ok(())
}

fn find_nfc_equivalent(dir: &Path, target_nfc: &str) -> Option<PathBuf> {
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .find(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.nfc().collect::<String>() == target_nfc)
                .unwrap_or(false)
        })
        .map(|e| e.path())
}

fn updated(filepath: &Path, new_modified: &str) -> bool {
    (|| -> Result<bool> {
        let old_modified = std::fs::metadata(filepath)?.modified()?;
        let new_modified = std::time::SystemTime::from(DateTime::parse_from_rfc3339(new_modified)?);
        let updated = old_modified < new_modified;
        if updated {
            println!("Found update for {filepath:?}.");
        }
        Ok(updated)
    })()
    .unwrap_or(false)
}

fn output_folder_name(id: u32, name: &str) -> String {
    sanitize_filename::sanitize(format!("d{id}_{name}"))
}

fn output_name(id: u32, display_name: &str) -> String {
    let name = if id == 0 {
        sanitize_filename::sanitize(display_name)
    } else {
        sanitize_filename_with_id(id, display_name)
    };
    name.nfc().collect()
}

pub fn filter_files(options: &ProcessOptions, path: &Path, files: Vec<File>) -> Vec<File> {
    // only download files that do not exist or are updated
    files
        .into_iter()
        .map(|mut f| {
            let nfc_name = output_name(f.id, &f.display_name);
            if f.id != 0 {
                f.display_name = nfc_name.clone();
            }
            let mut filepath = path.join(&nfc_name);
            // Canvas may hand back the same filename in different Unicode
            // normalization forms across runs (e.g. NFC vs NFD for "ú"). On
            // byte-level filesystems these are distinct entries. If the NFC
            // path isn't present, probe the directory for any entry that is
            // canonically equivalent and reuse its path so we don't create a
            // duplicate.
            if !nfc_name.is_ascii()
                && !filepath.exists()
                && let Some(existing) = find_nfc_equivalent(path, &nfc_name)
            {
                filepath = existing;
            }
            f.filepath = filepath;
            f
        })
        .filter(|f| !f.locked_for_user)
        .filter(|f| {
            if DateTime::parse_from_rfc3339(&f.updated_at).is_ok() {
                return true;
            }
            tracing::error!(
                "Failed to parse updated_at time for {}, {}",
                f.display_name,
                f.updated_at
            );
            false
        })
        .filter(|f| {
            !f.filepath.exists() || (updated(&f.filepath, &f.updated_at) && options.download_newer)
        })
        .filter(|f| {
            !ignored(
                &f.filepath,
                false,
                &options.base_path,
                options.ignore_matcher.as_deref(),
            )
        })
        .collect()
}

fn source_identity(file: &File) -> String {
    if file.id != 0 {
        return format!("canvas-file:{}", file.id);
    }
    reqwest::Url::parse(&file.url)
        .map(|mut url| {
            url.set_query(None);
            url.set_fragment(None);
            format!("url:{url}")
        })
        .unwrap_or_else(|_| format!("url:{}", file.url.split('?').next().unwrap_or(&file.url)))
}

pub fn enforce_unique_destinations(files: &mut Vec<File>) -> Result<()> {
    let mut destinations: HashMap<PathBuf, String> = HashMap::new();
    let mut unique = Vec::with_capacity(files.len());
    for file in files.drain(..) {
        let identity = source_identity(&file);
        match destinations.get(&file.filepath) {
            Some(existing) if existing == &identity => {}
            Some(existing) => anyhow::bail!(
                "conflicting download destination {}: sources {existing} and {identity}",
                file.filepath.display()
            ),
            None => {
                destinations.insert(file.filepath.clone(), identity);
                unique.push(file);
            }
        }
    }
    *files = unique;
    Ok(())
}

pub async fn process_file_id(
    (url, path): (String, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<File> {
    let file_resp = get_canvas_api(url.clone(), &options).await?;
    let file_result = file_resp.json::<File>().await;
    match file_result {
        Ok(mut file) => {
            let sanitized_filename = sanitize_filename::sanitize(&file.display_name);
            let file_path = path.join(sanitized_filename);
            file.filepath = file_path;
            Ok(file)
        }
        Err(e) => {
            tracing::error!("Error when getting file info at link:{url}, path:{path:?}\n{e:?}",);
            Err(Into::into(e))
        }
    }
}

pub async fn prepare_link_for_download(
    (link, path): (String, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<File> {
    let resp = options
        .client
        .head(&link)
        .bearer_auth(&options.canvas_token)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "Unable to inspect embedded file {link}: HTTP {}",
            resp.status()
        );
    }
    let headers = resp.headers();
    // get filename out of Content-Disposition header
    let filename = headers
        .get(header::CONTENT_DISPOSITION)
        .and_then(|x| x.to_str().ok())
        .and_then(|x| regex!(r#"filename="([^"]+)""#).captures(x))
        .and_then(|x| x.get(1))
        .map(|x| x.as_str().to_string())
        .unwrap_or_else(|| filename_from_url(&link));
    // last-modified header to TZ string
    let updated_at = headers
        .get(header::LAST_MODIFIED)
        .and_then(|x| x.to_str().ok())
        .and_then(|x| {
            let dt = DateTime::parse_from_rfc2822(x).ok()?;
            Some(dt.with_timezone(&Local).to_rfc3339())
        })
        .unwrap_or_else(|| Local::now().to_rfc3339());

    let sanitized_filename = sanitize_filename::sanitize(&filename);
    let file = File {
        id: 0,
        folder_id: None,
        display_name: filename,
        size: 0,
        url: link.clone(),
        updated_at,
        locked_for_user: false,
        filepath: path.join(sanitized_filename),
    };
    Ok(file)
}

fn filename_from_url(link: &str) -> String {
    reqwest::Url::parse(link)
        .ok()
        .and_then(|url| {
            url.path_segments()?
                .rev()
                .find(|segment| !segment.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(id: u32, url: &str, destination: &str) -> File {
        File {
            id,
            folder_id: None,
            display_name: "x".into(),
            size: 1,
            url: url.into(),
            updated_at: "2020-01-01T00:00:00Z".into(),
            locked_for_user: false,
            filepath: destination.into(),
        }
    }

    #[test]
    fn canvas_file_output_names_have_one_identity_prefix() {
        assert_eq!(output_name(42, "week/one"), "42_weekone");
        assert_eq!(output_name(42, "file_42_week/one"), "42_file_42_weekone");
    }

    #[test]
    fn canvas_folder_output_names_use_a_short_directory_prefix() {
        assert_eq!(output_folder_name(23, "Week/One"), "d23_WeekOne");
    }

    #[test]
    fn canvas_file_ids_disambiguate_colliding_raw_names() {
        assert_ne!(output_name(42, "week/one"), output_name(43, "weekone"));
    }

    #[test]
    fn id_zero_output_names_remain_untyped() {
        assert_eq!(output_name(0, "week/one"), "weekone");
    }

    #[test]
    fn url_fallback_filename_excludes_query_and_fragment() {
        assert_eq!(
            filename_from_url("https://canvas.example/files/42/download?hidden=1&wrap=1#preview"),
            "download"
        );
    }

    #[test]
    fn url_fallback_filename_uses_last_nonempty_path_segment() {
        assert_eq!(
            filename_from_url("https://canvas.example/images/avatar.png/"),
            "avatar.png"
        );
        assert_eq!(filename_from_url("not a URL"), "unknown");
    }

    #[test]
    fn same_destination_and_canvas_id_deduplicates() {
        let mut duplicates = vec![
            file(7, "https://a/one?sig=1", "out"),
            file(7, "https://a/two", "out"),
        ];
        enforce_unique_destinations(&mut duplicates)
            .expect("same Canvas identity should deduplicate");
        assert_eq!(duplicates.len(), 1);
    }

    #[test]
    fn same_idless_url_ignores_query_and_fragment() {
        let mut url_duplicates = vec![
            file(0, "https://a/image?sig=1", "img"),
            file(0, "https://a/image?sig=2#part", "img"),
        ];
        enforce_unique_destinations(&mut url_duplicates).expect("query strings are not identity");
        assert_eq!(url_duplicates.len(), 1);
    }

    #[test]
    fn differing_canvas_ids_conflict() {
        let mut conflict = vec![
            file(1, "https://a/one", "out"),
            file(2, "https://a/two", "out"),
        ];
        assert!(enforce_unique_destinations(&mut conflict).is_err());
    }

    #[test]
    fn differing_url_scheme_or_explicit_port_conflicts() {
        for other in ["http://a/image", "https://a:8443/image"] {
            let mut files = vec![file(0, "https://a/image", "img"), file(0, other, "img")];
            assert!(enforce_unique_destinations(&mut files).is_err());
        }
    }

    #[test]
    fn distinct_destinations_retain_input_order() {
        let mut files = vec![
            file(3, "https://a/three", "third"),
            file(1, "https://a/one", "first"),
            file(2, "https://a/two", "second"),
        ];
        enforce_unique_destinations(&mut files).expect("destinations are distinct");
        assert_eq!(
            files.iter().map(|file| file.id).collect::<Vec<_>>(),
            vec![3, 1, 2]
        );
    }
}
