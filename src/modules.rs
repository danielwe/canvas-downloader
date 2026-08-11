use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};

use crate::api::get_pages;
use crate::canvas::{File, ModuleItemResult, ModuleResult, ProcessOptions};
use crate::files::{filter_files, process_file_id};
use crate::pages::process_page_body;
use crate::utils::{
    create_folder_if_not_exist_or_ignored, get_raw_json_path, sanitize_filename_with_id,
};

fn append_raw_json_page(raw: &mut Vec<serde_json::Value>, body: &str) -> serde_json::Result<()> {
    let mut page = serde_json::from_str::<Vec<serde_json::Value>>(body)?;
    raw.append(&mut page);
    Ok(())
}

pub async fn process_modules(
    (url, path): (String, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    let modules_url = format!("{}modules", url);
    let pages = get_pages(modules_url, &options).await?;

    let mut all_modules = Vec::new();
    let mut raw_modules = Vec::new();

    for page in pages {
        let module_body = page.body;
        let module_result = serde_json::from_str::<ModuleResult>(&module_body);

        match module_result {
            Ok(ModuleResult::Ok(modules)) => {
                append_raw_json_page(&mut raw_modules, &module_body)
                    .with_context(|| format!("Unable to preserve raw modules from {url}"))?;
                all_modules.extend(modules);
            }

            Ok(ModuleResult::Err { status }) => {
                anyhow::bail!("Failed to access modules at {url}, status: {status}");
            }

            Err(e) => {
                return Err(e).with_context(|| format!("Unable to parse modules from {url}"));
            }
        };
    }

    if all_modules.is_empty() {
        return Ok(());
    }

    let modules_path = path.join("modules");
    if !create_folder_if_not_exist_or_ignored(&modules_path, &options)? {
        return Ok(());
    }

    if let Some(module_json) =
        get_raw_json_path(&path, "modules.json", &options.base_path, options.save_json)?
    {
        let mut module_file = std::fs::File::create(&module_json)
            .with_context(|| format!("Unable to create file for {module_json:?}"))?;
        module_file.write_all(serde_json::to_string_pretty(&raw_modules)?.as_bytes())?;
    }

    for module in all_modules {
        let module_path = modules_path.join(sanitize_filename_with_id(module.id, &module.name));
        if !create_folder_if_not_exist_or_ignored(&module_path, &options)? {
            continue;
        }
        fork!(
            process_module_items,
            (module.items_url, module_path),
            (String, PathBuf),
            options.clone()
        );
    }

    tracing::debug!(
        "📦 Modules synced for {}",
        path.file_name().unwrap_or_default().to_string_lossy()
    );
    options.n_modules.fetch_add(1, Ordering::Relaxed);

    Ok(())
}

async fn process_module_items(
    (url, path): (String, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    let pages = get_pages(url.clone(), &options).await?;
    let mut items = Vec::new();
    let mut raw_items = Vec::new();

    for page in pages {
        let items_body = page.body;
        let items_result = serde_json::from_str::<ModuleItemResult>(&items_body);

        match items_result {
            Ok(ModuleItemResult::Ok(page_items)) => {
                append_raw_json_page(&mut raw_items, &items_body)
                    .with_context(|| format!("Unable to preserve raw module items from {url}"))?;
                items.extend(page_items);
            }

            Ok(ModuleItemResult::Err { status }) => {
                anyhow::bail!(
                    "Failed to access module items at link:{url}, path:{path:?}, status:{status}"
                );
            }

            Err(e) => {
                return Err(e).with_context(|| {
                    format!("Unable to parse module items at link:{url}, path:{path:?}")
                });
            }
        }
    }

    if let Some(items_json) = get_raw_json_path(
        &path,
        "module_items.json",
        &options.base_path,
        options.save_json,
    )? {
        let mut items_file = std::fs::File::create(items_json.clone())
            .with_context(|| format!("Unable to create file for {:?}", items_json))?;
        let pretty_json = serde_json::to_string_pretty(&raw_items)
            .context("Unable to serialize aggregated module items")?;
        items_file
            .write_all(pretty_json.as_bytes())
            .with_context(|| format!("Unable to write to file for {:?}", items_json))?;
    }

    // Items in a Canvas module are returned as a flat list; a
    // `SubHeader` item starts a section that owns every following
    // item until the next `SubHeader`. `current_section` is the
    // destination folder for the section we're currently in:
    // `Some(path)` for items before any subheader, `Some(sub)`
    // while inside a subheader, or `None` if the active subheader
    // folder is ignored (skip its contents too).
    let mut current_section: Option<PathBuf> = Some(path.clone());
    let mut files_to_process: Vec<(PathBuf, File)> = Vec::new();

    for item in items {
        let item_name = sanitize_filename_with_id(item.id, &item.title);
        match item.item_type.as_str() {
            "File" => {
                let Some(section_path) = current_section.as_ref() else {
                    continue;
                };
                if let Some(content_id) = item.content_id {
                    let file_url = format!(
                        "{}/api/v1/files/{}",
                        options.canvas_url.trim_end_matches('/'),
                        content_id
                    );

                    match process_file_id((file_url, section_path.clone()), options.clone()).await {
                        Ok(file) => {
                            files_to_process.push((section_path.clone(), file));
                        }
                        Err(e) => {
                            tracing::error!("Error processing module file {}: {:?}", content_id, e);
                        }
                    }
                }
            }
            "Page" => {
                let Some(section_path) = current_section.as_ref() else {
                    continue;
                };
                if let Some(full_page_url) = item.url {
                    let item_path = section_path.join(item_name);
                    if !create_folder_if_not_exist_or_ignored(&item_path, &options)? {
                        continue;
                    }

                    fork!(
                        process_page_body,
                        (full_page_url, item_path),
                        (String, PathBuf),
                        options.clone()
                    );
                }
            }
            "Assignment" => {
                if let Some(content_id) = item.content_id {
                    tracing::debug!(
                        "Module item {} references assignment {}",
                        item.title,
                        content_id
                    );
                }
            }
            "Discussion" => {
                if let Some(content_id) = item.content_id {
                    tracing::debug!(
                        "Module item {} references discussion {}",
                        item.title,
                        content_id
                    );
                }
            }
            "ExternalUrl" => {
                let Some(section_path) = current_section.as_ref() else {
                    continue;
                };
                if let Some(external_url) = &item.external_url {
                    let url_file = section_path.join(format!("{item_name}.url"));
                    if let Ok(mut file) = std::fs::File::create(&url_file) {
                        let _ = writeln!(file, "[InternetShortcut]");
                        let _ = writeln!(file, "URL={}", external_url);
                    }
                }
            }
            "SubHeader" => {
                // SubHeader starts a new section. Subheader folders
                // are siblings under the module folder, not nested
                // inside the previous section.
                let subheader_path = path.join(item_name);
                if !create_folder_if_not_exist_or_ignored(&subheader_path, &options)? {
                    current_section = None;
                    continue;
                }
                current_section = Some(subheader_path);
            }
            _ => {
                tracing::error!(
                    "Unsupported module item type '{}' for item '{}'",
                    item.item_type,
                    item.title
                );
            }
        }
    }

    // Group queued files by destination section, then filter each
    // group against its own folder before extending the global
    // download queue in one lock acquisition.
    if !files_to_process.is_empty() {
        let mut by_section: HashMap<PathBuf, Vec<File>> = HashMap::new();
        for (section_path, file) in files_to_process {
            by_section.entry(section_path).or_default().push(file);
        }
        let mut all_filtered: Vec<File> = Vec::new();
        for (section_path, mut files) in by_section {
            let mut seen = HashSet::new();
            files.retain(|file| seen.insert(file.id));
            all_filtered.extend(filter_files(&options, &section_path, files));
        }
        if !all_filtered.is_empty() {
            let mut lock = options.files_to_download.lock().await;
            lock.extend(all_filtered);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_json_pages_preserve_api_order_unknown_fields_and_page_boundaries() {
        let mut aggregate = Vec::new();
        append_raw_json_page(
            &mut aggregate,
            r#"[{"id":1,"unknown":{"kept":true}},{"id":2,"type":"SubHeader"}]"#,
        )
        .expect("page one");
        append_raw_json_page(&mut aggregate, r#"[{"id":3,"type":"Page"}]"#).expect("page two");

        assert_eq!(
            aggregate
                .iter()
                .map(|item| item["id"].as_u64())
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );
        assert_eq!(aggregate[0]["unknown"]["kept"], true);
        assert_eq!(aggregate[1]["type"], "SubHeader");
        assert_eq!(aggregate[2]["type"], "Page");
    }

    #[test]
    fn raw_json_empty_pages_append_nothing() {
        let mut aggregate = vec![serde_json::json!({"id": 1})];
        append_raw_json_page(&mut aggregate, "[]").expect("empty array");
        assert_eq!(aggregate, vec![serde_json::json!({"id": 1})]);
    }

    #[test]
    fn raw_json_pages_reject_malformed_and_object_responses() {
        for body in ["not json", r#"{"id":1}"#] {
            assert!(append_raw_json_page(&mut Vec::new(), body).is_err());
        }
    }
}
