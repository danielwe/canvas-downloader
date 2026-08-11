use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};

use crate::api::{get_canvas_api, get_pages};
use crate::canvas::{PageBody, PageResult, ProcessOptions};
use crate::html::process_html_links;
use crate::utils::{
    create_folder_if_not_exist_or_ignored, get_raw_json_path, prettify_json,
    sanitize_filename_with_id,
};

pub async fn process_pages(
    (url, path): (String, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    let pages_url = format!("{}pages", url);
    let pages = get_pages(pages_url, &options).await?;

    let mut has_pages = false;
    let mut pages_folder_path = None;

    for pg in pages {
        let uri = pg.url;
        let page_body = pg.body;

        let page_result = serde_json::from_str::<PageResult>(&page_body);

        match page_result {
            Ok(PageResult::Ok(pages)) => {
                if !pages.is_empty() && !has_pages {
                    // Create pages folder only when we have actual pages
                    let pages_path = path.join("pages");
                    if !create_folder_if_not_exist_or_ignored(&pages_path, &options)? {
                        continue;
                    }
                    pages_folder_path = Some(pages_path.clone());
                    has_pages = true;

                    // Create pages.json file
                    if let Some(pages_json_path) = get_raw_json_path(
                        &path,
                        "pages.json",
                        &options.base_path,
                        options.save_json,
                    )? {
                        let mut pages_file = std::fs::File::create(pages_json_path.clone())
                            .with_context(|| {
                                format!("Unable to create file for {:?}", pages_json_path)
                            })?;
                        let pretty_json = prettify_json(&page_body).unwrap_or(page_body.clone());
                        pages_file
                            .write_all(pretty_json.as_bytes())
                            .with_context(|| {
                                format!("Could not write to file {:?}", pages_json_path)
                            })?;
                    }
                }

                for page in pages {
                    if let Some(ref pages_path) = pages_folder_path {
                        let page_url = format!("{}pages/{}", url, page.url);
                        fork!(
                            process_page_body,
                            (page_url, pages_path.clone()),
                            (String, PathBuf),
                            options.clone()
                        )
                    }
                }
            }

            Ok(PageResult::Err { status }) => {
                tracing::debug!("No pages found for url {} (status: {})", uri, status);
            }

            Err(e) => {
                tracing::debug!("No pages found for url {} (error: {})", uri, e);
            }
        };
    }

    if has_pages {
        tracing::debug!(
            "📄 Pages synced for {}",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
        options.n_pages.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

pub async fn process_page_body(
    (url, path): (String, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    let page_resp = get_canvas_api(url.clone(), &options).await?;
    let page_resp_text = page_resp.text().await?;

    let page_body_result = serde_json::from_str::<PageBody>(&page_resp_text);
    match page_body_result {
        Result::Ok(page_body) => {
            let page_name = sanitize_filename_with_id(page_body.page_id, &page_body.title);
            if let Some(page_file_path) = get_raw_json_path(
                &path,
                &format!("{page_name}.json"),
                &options.base_path,
                options.save_json,
            )? {
                let mut page_file = std::fs::File::create(page_file_path.clone())
                    .with_context(|| format!("Unable to create file for {:?}", page_file_path))?;

                let pretty_json = prettify_json(&page_resp_text).unwrap_or(page_resp_text.clone());
                page_file
                    .write_all(pretty_json.as_bytes())
                    .with_context(|| format!("Could not write to file {:?}", page_file_path))?;
            }

            let page_html = format!(
                "<html><head><title>{}</title></head><body>{}</body></html>",
                page_body.title,
                page_body.body.unwrap_or_default()
            );

            let page_html_path = path.join(format!("{page_name}.html"));
            let mut page_html_file = std::fs::File::create(page_html_path.clone())
                .with_context(|| format!("Unable to create file for {:?}", page_html_path))?;

            page_html_file
                .write_all(page_html.as_bytes())
                .with_context(|| format!("Could not write to file {:?}", page_html_path))?;

            fork!(
                process_html_links,
                (page_html, path, page_name),
                (String, PathBuf, String),
                options.clone()
            )
        }
        Result::Err(e) => {
            tracing::error!("Error when parsing page body at link:{url}, path:{path:?}\n{e:?}",);
        }
    }
    Ok(())
}
