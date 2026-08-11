use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::{Result, anyhow};
use chrono::{TimeZone, Utc};
use lazy_regex::regex;
use m3u8_rs::Playlist;
use reqwest::{Url, header};
use select::document::Document;
use select::predicate::Name;
use serde_json::json;

use crate::api::get_canvas_api;
use crate::canvas::{File, PanoptoDeliveryInfo, PanoptoSessionInfo, ProcessOptions, Session};
use crate::files::filter_files;
use crate::utils::{create_folder_if_not_exist_or_ignored, get_raw_json_path, prettify_json};

pub async fn process_videos(
    (url, id, path): (String, u32, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    let session = get_canvas_api(
        format!(
            "{}/login/session_token?return_to={}/courses/{}/external_tools/128",
            url, url, id
        ),
        &options,
    )
    .await?;
    let session_result = session.json::<Session>().await?;

    // Need a new client for each session for the cookie store
    let user_agent = format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    let client = reqwest::ClientBuilder::new()
        .user_agent(user_agent)
        .cookie_store(true)
        .build()?;
    let videos = client.get(session_result.session_url).send().await?;

    // Parse the form that contains the parameters needed to request
    let video_html = videos.text().await?;
    let (action, params) = {
        let panopto_document = Document::from_read(video_html.as_bytes())?;
        let panopto_form = panopto_document
            .find(Name("form"))
            .find(|n| n.attr("data-tool-id") == Some("mediaweb.ap.panopto.com"));

        // If no Panopto form found, course doesn't use Panopto
        let panopto_form = match panopto_form {
            Some(form) => form,
            None => {
                tracing::debug!("No Panopto videos found for course");
                return Ok(());
            }
        };

        let action = panopto_form
            .attr("action")
            .ok_or(anyhow!("Could not find panopto form action"))?
            .to_string();
        let params = panopto_form
            .find(Name("input"))
            .filter_map(|n| {
                n.attr("name")
                    .map(|name| (name.to_string(), n.attr("value").unwrap_or("").to_string()))
            })
            .collect::<Vec<(_, _)>>();
        (action, params)
    };
    // set origin and referral headers
    let panopto_response = client
        .post(action)
        .header("Origin", &url)
        .header("Referer", format!("{}/", url))
        .form(&params)
        .send()
        .await?;

    // parse location header as url
    let panopto_location = Url::parse(
        panopto_response
            .headers()
            .get(header::LOCATION)
            .ok_or(anyhow!("No location header"))?
            .to_str()?,
    )?;
    // get folderID from query string
    let panopto_folder_id = panopto_location
        .query_pairs()
        .find(|(key, _)| key == "folderID")
        .map(|(_, value)| value)
        .ok_or(anyhow!("Could not get Panopto Folder ID"))?
        .to_string();
    let panopto_host = panopto_location
        .host_str()
        .ok_or(anyhow!("Could not get Panopto Host"))?
        .to_string();

    let video_folder_path = path.join("videos");
    if !create_folder_if_not_exist_or_ignored(&video_folder_path, &options)? {
        return Ok(());
    }
    tracing::debug!(
        "🎬 Videos synced for {}",
        path.file_name().unwrap_or_default().to_string_lossy()
    );
    options.n_videos.fetch_add(1, Ordering::Relaxed);

    process_video_folder(
        (
            panopto_host,
            panopto_folder_id,
            client.clone(),
            video_folder_path,
        ),
        options,
    )
    .await?;

    Ok(())
}

async fn process_video_folder(
    (host, id, client, path): (String, String, reqwest::Client, PathBuf),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    // POST json folderID: to https://mediaweb.ap.panopto.com/Panopto/Services/Data.svc/GetFolderInfo
    let folderinfo_result = client
        .post(format!(
            "https://{}/Panopto/Services/Data.svc/GetFolderInfo",
            host
        ))
        .json(&json!({
            "folderID": id,
        }))
        .send()
        .await?;
    // write into folder.json
    let folderinfo = folderinfo_result.text().await?;
    if let Some(folder_json_path) =
        get_raw_json_path(&path, "folder.json", &options.base_path, options.save_json)?
    {
        let mut file = std::fs::File::create(folder_json_path)?;
        let pretty_json = prettify_json(&folderinfo).unwrap_or(folderinfo.clone());
        file.write_all(pretty_json.as_bytes())?;
    }

    // write into sessions.json
    let sessions_json = get_raw_json_path(
        &path,
        "sessions.json",
        &options.base_path,
        options.save_json,
    )?;
    let mut aggregated_sessions: Option<serde_json::Value> = None;

    for i in 0.. {
        let sessions_result = client
            .post(format!(
                "https://{}/Panopto/Services/Data.svc/GetSessions",
                host
            ))
            .json(&json!({
                "queryParameters":
                {
                    "query":null,
                    "sortColumn":1,
                    "sortAscending":false,
                    "maxResults":100,
                    "page":i,
                    "startDate":null,
                    "endDate":null,
                    "folderID":id,
                    "bookmarked":false,
                    "getFolderData":true,
                    "isSharedWithMe":false,
                    "isSubscriptionsPage":false,
                    "includeArchived":true,
                    "includeArchivedStateCount":true,
                    "sessionListOnlyArchived":false,
                    "includePlaylists":true
                }
            }))
            .send()
            .await?;

        let sessions_text = sessions_result.text().await?;
        let folder_sessions = serde_json::from_str::<serde_json::Value>(&sessions_text)?;
        let folder_sessions_results = folder_sessions
            .get("d")
            .ok_or(anyhow!("Could not get Panopto Folder Sessions"))?;

        aggregate_panopto_page(&mut aggregated_sessions, folder_sessions.clone())?;
        let sessions =
            serde_json::from_value::<PanoptoSessionInfo>(folder_sessions_results.clone())?;

        // Subfolders are the same, so process only the first request
        if i == 0 {
            for subfolder in sessions.Subfolders {
                let subfolder_path = path.join(panopto_folder_name(&subfolder.ID, &subfolder.Name));
                if !create_folder_if_not_exist_or_ignored(&subfolder_path, &options)? {
                    continue;
                }
                fork!(
                    process_video_folder,
                    (host.clone(), subfolder.ID, client.clone(), subfolder_path),
                    (String, String, reqwest::Client, PathBuf),
                    options.clone()
                );
            }
        }
        // End of page results
        if sessions.Results.is_empty() {
            break;
        }
        for result in sessions.Results {
            fork!(
                process_session,
                (host.clone(), result, client.clone(), path.clone()),
                (
                    String,
                    crate::canvas::PanoptoResult,
                    reqwest::Client,
                    PathBuf
                ),
                options.clone()
            )
        }
    }
    if let (Some(sessions_path), Some(aggregated)) = (sessions_json, aggregated_sessions) {
        let mut file = std::fs::File::create(sessions_path)?;
        file.write_all(serde_json::to_string_pretty(&aggregated)?.as_bytes())?;
    }
    Ok(())
}

fn aggregate_panopto_page(
    aggregate: &mut Option<serde_json::Value>,
    page: serde_json::Value,
) -> Result<()> {
    if aggregate.is_none() {
        *aggregate = Some(page);
        return Ok(());
    }
    let page_results = page
        .pointer("/d/Results")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("Panopto page has no Results array"))?;
    let results = aggregate
        .as_mut()
        .and_then(|v| v.pointer_mut("/d/Results"))
        .and_then(|v| v.as_array_mut())
        .ok_or_else(|| anyhow!("aggregated Panopto response has no Results array"))?;
    results.extend(page_results.iter().cloned());
    Ok(())
}

fn panopto_folder_name(id: &str, name: &str) -> String {
    sanitize_filename::sanitize(format!("d{id}_{name}"))
}

fn panopto_session_name(result: &crate::canvas::PanoptoResult) -> String {
    sanitize_filename::sanitize(format!("{}_{}", result.DeliveryID, result.SessionName))
}

async fn process_session(
    (host, result, client, path): (
        String,
        crate::canvas::PanoptoResult,
        reqwest::Client,
        PathBuf,
    ),
    options: Arc<ProcessOptions>,
) -> Result<()> {
    // POST deliveryID: to https://mediaweb.ap.panopto.com/Panopto/Pages/Viewer/DeliveryInfo.aspx
    let resp = client
        .post(format!(
            "https://{}/Panopto/Pages/Viewer/DeliveryInfo.aspx",
            host
        ))
        .form(&[
            ("deliveryId", result.DeliveryID.as_str()),
            ("invocationId", ""),
            ("isLiveNotes", "false"),
            ("refreshAuthCookie", "true"),
            ("isActiveBroadcast", "false"),
            ("isEditing", "false"),
            ("isKollectiveAgentInstalled", "false"),
            ("isEmbed", "false"),
            ("responseType", "json"),
        ])
        .send()
        .await?;

    let delivery_info = resp.json::<PanoptoDeliveryInfo>().await?;

    let viewer_file_id = delivery_info.ViewerFileId;
    let panopto_url = Url::parse(&result.IosVideoUrl)?;
    let panopto_cdn_host = panopto_url
        .host_str()
        .unwrap_or("s-cloudfront.cdn.ap.panopto.com");
    let panopto_master_m3u8 = format!(
        "https://{}/sessions/{}/{}-{}.hls/master.m3u8",
        panopto_cdn_host, result.SessionID, result.DeliveryID, viewer_file_id
    );
    let m3u8_resp = client.get(panopto_master_m3u8).send().await?;
    let m3u8_text = m3u8_resp.text().await?;
    let m3u8_parser = m3u8_rs::parse_playlist_res(m3u8_text.as_bytes());
    match m3u8_parser {
        Ok(Playlist::MasterPlaylist(pl)) => {
            // get the highest bandwidth
            let download_variant = pl
                .variants
                .iter()
                .max_by_key(|v| v.bandwidth)
                .expect("No variants found");

            let panopto_index_m3u8 = format!(
                "https://{}/sessions/{}/{}-{}.hls/{}",
                panopto_cdn_host,
                result.SessionID,
                result.DeliveryID,
                viewer_file_id,
                download_variant.uri
            );

            let index_m3u8_resp = client.get(panopto_index_m3u8).send().await?;
            let index_m3u8_text = index_m3u8_resp.text().await?;
            let index_m3u8_parser = m3u8_rs::parse_playlist_res(index_m3u8_text.as_bytes());
            match index_m3u8_parser {
                Ok(Playlist::MasterPlaylist(_index_pl)) => {}
                Ok(Playlist::MediaPlaylist(index_pl)) => {
                    let uri_id = download_variant
                        .uri
                        .split("/")
                        .next()
                        .ok_or(anyhow!("Could not get URI ID"))?;
                    let file_uri = index_pl.segments[0].uri.clone();
                    let file_uri_ext = Path::new(&file_uri)
                        .extension()
                        .unwrap_or(OsStr::new(""))
                        .to_str()
                        .unwrap_or("");
                    let panopto_mp4_file = format!(
                        "https://{}/sessions/{}/{}-{}.hls/{}/{}",
                        panopto_cdn_host,
                        result.SessionID,
                        result.DeliveryID,
                        viewer_file_id,
                        uri_id,
                        file_uri
                    );
                    let session_name = panopto_session_name(&result);
                    let download_file_name = if file_uri_ext.is_empty() {
                        session_name
                    } else {
                        format!("{}.{}", session_name, file_uri_ext)
                    };

                    let date_match_rfc3339 = regex!(r"/Date\((\d+)\)/")
                        .captures(&result.StartTime)
                        .and_then(|x| x.get(1))
                        .map(|x| x.as_str())
                        .ok_or(anyhow!("Parse error for StartTime"))
                        .and_then(|x| {
                            x.parse::<i64>()
                                .map_err(|e| anyhow!("Conversion error for StartTime: {}", e))
                        })
                        .and_then(|x| {
                            Utc.timestamp_millis_opt(x)
                                .earliest()
                                .ok_or(anyhow!("Timestamp parse error for StartTime"))
                        })
                        .map(|x| x.to_rfc3339())?;

                    let file = File {
                        display_name: download_file_name,
                        folder_id: None,
                        id: 0,
                        size: 0,
                        url: panopto_mp4_file,
                        locked_for_user: false,
                        updated_at: date_match_rfc3339,
                        filepath: path.clone(),
                    };
                    let mut lock = options.files_to_download.lock().await;
                    let mut filtered_files = filter_files(&options, &path, [file].to_vec());
                    lock.append(&mut filtered_files);
                }
                Err(e) => tracing::error!("{e:?}"),
            }
        }
        Ok(Playlist::MediaPlaylist(_pl)) => {}
        Err(e) => tracing::error!("{e:?}"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panopto_pages_append_results_and_keep_first_subfolders() {
        let mut aggregate = None;
        aggregate_panopto_page(&mut aggregate, serde_json::json!({"d":{"Results":[{"id":1}],"Subfolders":[{"ID":"a"}],"TotalNumber":2}})).expect("first page");
        aggregate_panopto_page(
            &mut aggregate,
            serde_json::json!({"d":{"Results":[{"id":2}],"Subfolders":[{"ID":"a"}]}}),
        )
        .expect("second page");
        let value = aggregate.expect("aggregate");
        assert_eq!(
            value
                .pointer("/d/Results")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(
            value
                .pointer("/d/Subfolders")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            value.pointer("/d/TotalNumber").and_then(|v| v.as_u64()),
            Some(2)
        );
    }

    #[test]
    fn panopto_delivery_ids_disambiguate_one_session() {
        let result = |delivery_id: &str| crate::canvas::PanoptoResult {
            DeliveryID: delivery_id.into(),
            SessionID: "shared-session".into(),
            SessionName: "Lecture".into(),
            StartTime: String::new(),
            IosVideoUrl: String::new(),
        };
        assert_ne!(
            panopto_session_name(&result("delivery-1")),
            panopto_session_name(&result("delivery-2"))
        );
    }

    #[test]
    fn panopto_string_ids_are_short_and_sanitized() {
        assert_eq!(
            panopto_folder_name("folder/17", "Lectures/2026"),
            "dfolder17_Lectures2026"
        );
        let result = crate::canvas::PanoptoResult {
            DeliveryID: "delivery/17".into(),
            SessionID: String::new(),
            SessionName: "Lecture/One".into(),
            StartTime: String::new(),
            IosVideoUrl: String::new(),
        };
        assert_eq!(panopto_session_name(&result), "delivery17_LectureOne");
    }
}
