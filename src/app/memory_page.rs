//! The Memory Library settings page: browse, search, review, and clean up
//! AgentOS persistent memory across projects (Knowledge, Notes, and
//! Checkpoints) backed by `waku-daemon`'s SQLite store.

use std::path::Path;

use chrono::{DateTime, Local, Utc};
use gpui::ClickEvent;
use serde_json::Value;
use waku_client::memory::{
    MemoryCheckpointRecord, MemoryEntryKind, MemoryKnowledgeRecord, MemoryLibrarySnapshot,
    MemoryNoteRecord,
};

use super::*;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum MemoryTab {
    #[default]
    Knowledge,
    Notes,
    Checkpoints,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum MemoryScopeFilter {
    #[default]
    CurrentProject,
    AllProjects,
}

const MEMORY_RESCAN_AFTER: Duration = Duration::from_secs(30);

fn format_memory_timestamp(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| {
            dt.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| ts.to_string())
}

fn project_display_label(project_path: &str) -> String {
    Path::new(project_path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(project_path)
        .to_owned()
}

fn checkpoint_summary_lines(payload_json: &str) -> Vec<(String, Vec<String>)> {
    let Ok(val) = serde_json::from_str::<Value>(payload_json) else {
        return Vec::new();
    };
    let mut sections = Vec::new();
    for (key, label) in [
        ("completed", tr!("memory.cp_completed")),
        ("in_progress", tr!("memory.cp_in_progress")),
        ("next_actions", tr!("memory.cp_next_actions")),
        ("open_questions", tr!("memory.cp_open_questions")),
        ("constraints", tr!("memory.cp_constraints")),
        ("rejected_approaches", tr!("memory.cp_rejected")),
    ] {
        if let Some(items) = val.get(key).and_then(Value::as_array) {
            let texts: Vec<String> = items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect();
            if !texts.is_empty() {
                sections.push((label, texts));
            }
        }
    }
    sections
}

impl Waku {
    /// Load the full AgentOS memory library snapshot from `waku-daemon` off the
    /// UI thread. Filtering by project and search query happens locally so
    /// switching scopes or typing is instantaneous.
    pub(super) fn ensure_memory_library(&mut self, force: bool, cx: &mut Context<Self>) {
        if self.memory_loading {
            return;
        }
        let fresh = self.memory_library.is_some()
            && self
                .memory_loaded_at
                .is_some_and(|loaded| loaded.elapsed() < MEMORY_RESCAN_AFTER);
        if !force && fresh {
            return;
        }
        self.memory_loading = true;
        self.memory_load_generation += 1;
        let generation = self.memory_load_generation;
        let daemon = self.daemon.client();
        cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    match daemon.request(
                        Uuid::nil(),
                        Uuid::nil(),
                        waku_client::Command::LoadMemoryLibrary {
                            project_path: None,
                            query: None,
                        },
                    )? {
                        waku_client::ResponsePayload::MemoryLibrary { library } => Ok(library),
                        _ => anyhow::bail!("the daemon returned an invalid memory library response"),
                    }
                })
                .await;
            let _ = this.update(cx, |this, cx| {
                if this.memory_load_generation != generation {
                    return;
                }
                this.memory_loading = false;
                match result {
                    Ok(library) => {
                        this.memory_library = Some(Rc::new(library));
                        this.memory_loaded_at = Some(Instant::now());
                    }
                    Err(error) => this.show_toast(error.to_string()),
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn update_memory_knowledge_status(
        &mut self,
        knowledge_id: String,
        status: &'static str,
        cx: &mut Context<Self>,
    ) {
        let daemon = self.daemon.client();
        cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    daemon.request(
                        Uuid::nil(),
                        Uuid::nil(),
                        waku_client::Command::UpdateMemoryKnowledgeStatus {
                            knowledge_id,
                            status: status.to_owned(),
                        },
                    )?;
                    Ok::<_, anyhow::Error>(())
                })
                .await;
            let _ = this.update(cx, |this, cx| {
                if let Err(error) = result {
                    this.show_toast(error.to_string());
                } else {
                    this.memory_loaded_at = None;
                    this.ensure_memory_library(true, cx);
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn delete_memory_entry(
        &mut self,
        kind: MemoryEntryKind,
        id: String,
        note_id: Option<String>,
        scope: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let daemon = self.daemon.client();
        cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    daemon.request(
                        Uuid::nil(),
                        Uuid::nil(),
                        waku_client::Command::DeleteMemoryEntry {
                            kind,
                            id,
                            note_id,
                            scope,
                        },
                    )?;
                    Ok::<_, anyhow::Error>(())
                })
                .await;
            let _ = this.update(cx, |this, cx| {
                if let Err(error) = result {
                    this.show_toast(error.to_string());
                } else {
                    this.memory_loaded_at = None;
                    this.ensure_memory_library(true, cx);
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn current_memory_project_path(&self) -> Option<String> {
        self.selected_workspace_path()
            .map(|p| p.to_string_lossy().into_owned())
    }

    fn filtered_memory<'a>(
        &self,
        snapshot: &'a MemoryLibrarySnapshot,
        cx: &App,
    ) -> (
        Vec<&'a MemoryKnowledgeRecord>,
        Vec<&'a MemoryNoteRecord>,
        Vec<&'a MemoryCheckpointRecord>,
    ) {
        let active_project = if self.memory_scope_filter == MemoryScopeFilter::CurrentProject {
            self.current_memory_project_path()
        } else {
            None
        };
        let query = self.memory_search.read(cx).content().trim().to_lowercase();
        let matches_query = |fields: &[&str]| -> bool {
            query.is_empty()
                || fields
                    .iter()
                    .any(|field| field.to_lowercase().contains(&query))
        };

        let knowledge: Vec<&MemoryKnowledgeRecord> = snapshot
            .knowledge
            .iter()
            .filter(|item| {
                active_project
                    .as_deref()
                    .is_none_or(|proj| item.scope == "user" || item.project_path == proj)
            })
            .filter(|item| {
                matches_query(&[
                    &item.claim,
                    &item.knowledge_id,
                    &item.project_path,
                    &item.status,
                    item.session_title.as_deref().unwrap_or(""),
                ])
            })
            .collect();

        let notes: Vec<&MemoryNoteRecord> = snapshot
            .notes
            .iter()
            .filter(|item| {
                active_project
                    .as_deref()
                    .is_none_or(|proj| item.project_path == proj)
            })
            .filter(|item| {
                matches_query(&[
                    &item.note_id,
                    &item.body,
                    &item.project_path,
                    item.session_title.as_deref().unwrap_or(""),
                ])
            })
            .collect();

        let checkpoints: Vec<&MemoryCheckpointRecord> = snapshot
            .checkpoints
            .iter()
            .filter(|item| {
                active_project
                    .as_deref()
                    .is_none_or(|proj| item.project_path == proj)
            })
            .filter(|item| {
                matches_query(&[
                    &item.checkpoint_id,
                    &item.payload_json,
                    &item.project_path,
                    item.session_title.as_deref().unwrap_or(""),
                ])
            })
            .collect();

        (knowledge, notes, checkpoints)
    }

    pub(super) fn render_memory_settings(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = Theme::current(cx);
        let current_project = self.current_memory_project_path();
        let current_project_name = current_project
            .as_deref()
            .map(project_display_label)
            .unwrap_or_else(|| tr!("memory.current_project"));

        let empty_snapshot = MemoryLibrarySnapshot::default();
        let snapshot = self
            .memory_library
            .as_deref()
            .unwrap_or(&empty_snapshot);
        let (knowledge, notes, checkpoints) = self.filtered_memory(snapshot, cx);

        let scope_bar = div()
            .flex()
            .items_center()
            .justify_between()
            .gap(px(8.0))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(px(6.0))
                    .child(self.memory_pill_button(
                        "memory-scope-current",
                        format!("{} ({})", tr!("memory.current_project"), current_project_name),
                        self.memory_scope_filter == MemoryScopeFilter::CurrentProject,
                        cx.listener(|this, _, _, cx| {
                            this.memory_scope_filter = MemoryScopeFilter::CurrentProject;
                            cx.notify();
                        }),
                        &theme,
                    ))
                    .child(self.memory_pill_button(
                        "memory-scope-all",
                        tr!("memory.all_projects"),
                        self.memory_scope_filter == MemoryScopeFilter::AllProjects,
                        cx.listener(|this, _, _, cx| {
                            this.memory_scope_filter = MemoryScopeFilter::AllProjects;
                            cx.notify();
                        }),
                        &theme,
                    )),
            )
            .child(
                div()
                    .id("memory-refresh")
                    .h(px(28.0))
                    .px(px(10.0))
                    .rounded(px(7.0))
                    .border_1()
                    .border_color(theme.border)
                    .bg(theme.raised)
                    .flex()
                    .items_center()
                    .gap(px(6.0))
                    .cursor_default()
                    .text_size(sp(12.5))
                    .text_color(theme.text_secondary)
                    .hover(|el| el.bg(theme.overlay))
                    .active(|el| el.bg(theme.overlay_strong))
                    .child(icon("icons/rotate-cw.svg", 12.0, theme.text_tertiary))
                    .child(if self.memory_loading {
                        tr!("memory.refreshing")
                    } else {
                        tr!("memory.refresh")
                    })
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.ensure_memory_library(true, cx);
                    })),
            );

        let tabs = div()
            .flex()
            .items_center()
            .gap(px(6.0))
            .child(self.memory_pill_button(
                "memory-tab-knowledge",
                format!("{} ({})", tr!("memory.tab_knowledge"), knowledge.len()),
                self.memory_tab == MemoryTab::Knowledge,
                cx.listener(|this, _, _, cx| {
                    this.memory_tab = MemoryTab::Knowledge;
                    cx.notify();
                }),
                &theme,
            ))
            .child(self.memory_pill_button(
                "memory-tab-notes",
                format!("{} ({})", tr!("memory.tab_notes"), notes.len()),
                self.memory_tab == MemoryTab::Notes,
                cx.listener(|this, _, _, cx| {
                    this.memory_tab = MemoryTab::Notes;
                    cx.notify();
                }),
                &theme,
            ))
            .child(self.memory_pill_button(
                "memory-tab-checkpoints",
                format!("{} ({})", tr!("memory.tab_checkpoints"), checkpoints.len()),
                self.memory_tab == MemoryTab::Checkpoints,
                cx.listener(|this, _, _, cx| {
                    this.memory_tab = MemoryTab::Checkpoints;
                    cx.notify();
                }),
                &theme,
            ));

        let search_bar = div()
            .h(px(34.0))
            .px(px(10.0))
            .rounded(px(8.0))
            .border_1()
            .border_color(theme.border)
            .bg(theme.raised)
            .flex()
            .items_center()
            .gap(px(8.0))
            .child(icon("icons/search.svg", 13.0, theme.text_tertiary))
            .child(div().flex_1().min_w_0().child(self.memory_search.clone()));

        let mut list = div().flex().flex_col().gap(px(10.0));
        if self.memory_library.is_none() && self.memory_loading {
            list = list.child(self.memory_empty_card(tr!("memory.loading"), &theme));
        } else {
            match self.memory_tab {
                MemoryTab::Knowledge => {
                    if knowledge.is_empty() {
                        list = list.child(self.memory_empty_card(tr!("memory.empty_knowledge"), &theme));
                    } else {
                        for (idx, item) in knowledge.iter().enumerate() {
                            list = list.child(self.render_knowledge_card(idx, item, &theme, cx));
                        }
                    }
                }
                MemoryTab::Notes => {
                    if notes.is_empty() {
                        list = list.child(self.memory_empty_card(tr!("memory.empty_notes"), &theme));
                    } else {
                        for (idx, item) in notes.iter().enumerate() {
                            list = list.child(self.render_note_card(idx, item, &theme, cx));
                        }
                    }
                }
                MemoryTab::Checkpoints => {
                    if checkpoints.is_empty() {
                        list = list.child(self.memory_empty_card(tr!("memory.empty_checkpoints"), &theme));
                    } else {
                        for (idx, item) in checkpoints.iter().enumerate() {
                            list = list.child(self.render_checkpoint_card(idx, item, &theme, cx));
                        }
                    }
                }
            }
        }

        div()
            .mt(px(18.0))
            .flex()
            .flex_col()
            .gap(px(14.0))
            .child(
                div()
                    .text_size(sp(13.0))
                    .text_color(theme.text_secondary)
                    .child(tr!("memory.subtitle")),
            )
            .child(scope_bar)
            .child(tabs)
            .child(search_bar)
            .child(list)
            .into_any_element()
    }

    fn memory_pill_button(
        &self,
        id: &'static str,
        label: String,
        active: bool,
        on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
        theme: &Theme,
    ) -> Stateful<Div> {
        div()
            .id(id)
            .h(px(28.0))
            .px(px(10.0))
            .rounded(px(7.0))
            .border_1()
            .border_color(if active { theme.accent } else { theme.border })
            .bg(if active {
                theme.overlay_strong
            } else {
                theme.raised
            })
            .flex()
            .items_center()
            .cursor_default()
            .text_size(sp(12.5))
            .font_weight(if active {
                FontWeight::MEDIUM
            } else {
                FontWeight::NORMAL
            })
            .text_color(if active {
                theme.text
            } else {
                theme.text_secondary
            })
            .hover(|el| el.bg(theme.overlay))
            .child(SharedString::from(label))
            .on_click(on_click)
    }

    fn memory_empty_card(&self, message: String, theme: &Theme) -> Div {
        div()
            .p(px(20.0))
            .rounded(px(10.0))
            .border_1()
            .border_color(theme.border)
            .bg(theme.raised)
            .text_size(sp(13.0))
            .text_color(theme.text_tertiary)
            .child(SharedString::from(message))
    }

    fn render_session_jump_badge(
        &self,
        id_prefix: String,
        session_id: &str,
        session_title: Option<&str>,
        theme: &Theme,
        cx: &mut Context<Self>,
    ) -> Option<Stateful<Div>> {
        let parsed = Uuid::parse_str(session_id).ok()?;
        let exists = self.state.sessions.iter().any(|s| s.id == parsed);
        let label = session_title
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| session_id.chars().take(8).collect());
        Some(
            div()
                .id(SharedString::from(id_prefix))
                .px(px(6.0))
                .py(px(2.0))
                .rounded(px(5.0))
                .border_1()
                .border_color(theme.border)
                .bg(theme.surface)
                .max_w(px(220.0))
                .truncate()
                .text_size(sp(11.5))
                .text_color(if exists {
                    theme.accent
                } else {
                    theme.text_tertiary
                })
                .when(exists, |el| el.cursor_pointer().hover(|h| h.bg(theme.overlay)))
                .child(SharedString::from(label))
                .on_click(cx.listener(move |this, _, _, cx| {
                    if exists {
                        this.settings_page = None;
                        this.select_session(parsed, cx);
                    }
                })),
        )
    }

    fn render_knowledge_card(
        &self,
        idx: usize,
        item: &MemoryKnowledgeRecord,
        theme: &Theme,
        cx: &mut Context<Self>,
    ) -> Div {
        let (status_label, status_color) = match item.status.as_str() {
            "promoted" => (tr!("memory.status_promoted"), theme.success),
            "rejected" => (tr!("memory.status_rejected"), theme.danger),
            _ => (tr!("memory.status_candidate"), theme.warning),
        };
        let kid_promote = item.knowledge_id.clone();
        let kid_reject = item.knowledge_id.clone();
        let kid_delete = item.knowledge_id.clone();
        let is_promoted = item.status == "promoted";
        let is_rejected = item.status == "rejected";

        div()
            .p(px(14.0))
            .rounded(px(10.0))
            .border_1()
            .border_color(theme.border)
            .bg(theme.raised)
            .flex()
            .flex_col()
            .gap(px(10.0))
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap(px(8.0))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .flex_wrap()
                            .gap(px(6.0))
                            .child(
                                div()
                                    .px(px(6.0))
                                    .py(px(2.0))
                                    .rounded(px(5.0))
                                    .border_1()
                                    .border_color(status_color)
                                    .text_size(sp(11.5))
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(status_color)
                                    .child(SharedString::from(status_label)),
                            )
                            .child(
                                div()
                                    .px(px(6.0))
                                    .py(px(2.0))
                                    .rounded(px(5.0))
                                    .border_1()
                                    .border_color(theme.border)
                                    .text_size(sp(11.5))
                                    .text_color(theme.text_secondary)
                                    .child(SharedString::from(format!(
                                        "{} · {}",
                                        item.scope,
                                        project_display_label(&item.project_path)
                                    ))),
                            )
                            .children(self.render_session_jump_badge(
                                format!("mem-kn-sess-{idx}"),
                                &item.session_id,
                                item.session_title.as_deref(),
                                theme,
                                cx,
                            )),
                    )
                    .child(
                        div()
                            .flex_none()
                            .text_size(sp(11.5))
                            .text_color(theme.text_tertiary)
                            .child(SharedString::from(format_memory_timestamp(item.created_at))),
                    ),
            )
            .child(
                div()
                    .text_size(sp(13.0))
                    .text_color(theme.text)
                    .child(SharedString::from(item.claim.clone())),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_end()
                    .gap(px(6.0))
                    .when(!is_promoted, |row| {
                        row.child(
                            div()
                                .id(SharedString::from(format!("mem-kn-promote-{idx}")))
                                .h(px(26.0))
                                .px(px(9.0))
                                .rounded(px(6.0))
                                .border_1()
                                .border_color(theme.border)
                                .bg(theme.surface)
                                .flex()
                                .items_center()
                                .gap(px(4.0))
                                .cursor_default()
                                .text_size(sp(12.0))
                                .text_color(theme.success)
                                .hover(|el| el.bg(theme.overlay))
                                .child(icon("icons/check.svg", 12.0, theme.success))
                                .child(tr!("memory.action_promote"))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.update_memory_knowledge_status(
                                        kid_promote.clone(),
                                        "promoted",
                                        cx,
                                    );
                                })),
                        )
                    })
                    .when(!is_rejected, |row| {
                        row.child(
                            div()
                                .id(SharedString::from(format!("mem-kn-reject-{idx}")))
                                .h(px(26.0))
                                .px(px(9.0))
                                .rounded(px(6.0))
                                .border_1()
                                .border_color(theme.border)
                                .bg(theme.surface)
                                .flex()
                                .items_center()
                                .gap(px(4.0))
                                .cursor_default()
                                .text_size(sp(12.0))
                                .text_color(theme.text_secondary)
                                .hover(|el| el.bg(theme.overlay))
                                .child(icon("icons/x.svg", 12.0, theme.text_secondary))
                                .child(tr!("memory.action_reject"))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.update_memory_knowledge_status(
                                        kid_reject.clone(),
                                        "rejected",
                                        cx,
                                    );
                                })),
                        )
                    })
                    .child(
                        div()
                            .id(SharedString::from(format!("mem-kn-delete-{idx}")))
                            .h(px(26.0))
                            .px(px(9.0))
                            .rounded(px(6.0))
                            .border_1()
                            .border_color(theme.border)
                            .bg(theme.surface)
                            .flex()
                            .items_center()
                            .gap(px(4.0))
                            .cursor_default()
                            .text_size(sp(12.0))
                            .text_color(theme.danger)
                            .hover(|el| el.bg(theme.overlay))
                            .child(icon("icons/trash.svg", 12.0, theme.danger))
                            .child(tr!("memory.action_delete"))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.delete_memory_entry(
                                    MemoryEntryKind::Knowledge,
                                    kid_delete.clone(),
                                    None,
                                    None,
                                    cx,
                                );
                            })),
                    ),
            )
    }

    fn render_note_card(
        &self,
        idx: usize,
        item: &MemoryNoteRecord,
        theme: &Theme,
        cx: &mut Context<Self>,
    ) -> Div {
        let sid_delete = item.session_id.clone();
        let note_id_delete = item.note_id.clone();
        let scope_delete = item.scope.clone();
        let body_preview: String = if item.body.chars().count() > 1600 {
            let truncated: String = item.body.chars().take(1600).collect();
            format!("{truncated}…")
        } else {
            item.body.clone()
        };

        div()
            .p(px(14.0))
            .rounded(px(10.0))
            .border_1()
            .border_color(theme.border)
            .bg(theme.raised)
            .flex()
            .flex_col()
            .gap(px(8.0))
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap(px(8.0))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .flex_wrap()
                            .gap(px(6.0))
                            .child(
                                div()
                                    .text_size(sp(13.0))
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(theme.text)
                                    .child(SharedString::from(format!(
                                        "{} (v{})",
                                        item.note_id, item.revision
                                    ))),
                            )
                            .child(
                                div()
                                    .px(px(6.0))
                                    .py(px(2.0))
                                    .rounded(px(5.0))
                                    .border_1()
                                    .border_color(theme.border)
                                    .text_size(sp(11.5))
                                    .text_color(theme.text_secondary)
                                    .child(SharedString::from(format!(
                                        "{} · {}",
                                        item.scope,
                                        project_display_label(&item.project_path)
                                    ))),
                            )
                            .children(self.render_session_jump_badge(
                                format!("mem-note-sess-{idx}"),
                                &item.session_id,
                                item.session_title.as_deref(),
                                theme,
                                cx,
                            )),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(px(8.0))
                            .child(
                                div()
                                    .text_size(sp(11.5))
                                    .text_color(theme.text_tertiary)
                                    .child(SharedString::from(format_memory_timestamp(
                                        item.updated_at,
                                    ))),
                            )
                            .child(
                                div()
                                    .id(SharedString::from(format!("mem-note-delete-{idx}")))
                                    .h(px(24.0))
                                    .px(px(8.0))
                                    .rounded(px(6.0))
                                    .border_1()
                                    .border_color(theme.border)
                                    .bg(theme.surface)
                                    .flex()
                                    .items_center()
                                    .gap(px(4.0))
                                    .cursor_default()
                                    .text_size(sp(11.5))
                                    .text_color(theme.danger)
                                    .hover(|el| el.bg(theme.overlay))
                                    .child(icon("icons/trash.svg", 11.0, theme.danger))
                                    .child(tr!("memory.action_delete"))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.delete_memory_entry(
                                            MemoryEntryKind::Note,
                                            sid_delete.clone(),
                                            Some(note_id_delete.clone()),
                                            Some(scope_delete.clone()),
                                            cx,
                                        );
                                    })),
                            ),
                    ),
            )
            .child(
                div()
                    .p(px(10.0))
                    .rounded(px(7.0))
                    .bg(theme.surface)
                    .text_size(sp(12.5))
                    .text_color(theme.text_secondary)
                    .child(SharedString::from(body_preview)),
            )
    }

    fn render_checkpoint_card(
        &self,
        idx: usize,
        item: &MemoryCheckpointRecord,
        theme: &Theme,
        cx: &mut Context<Self>,
    ) -> Div {
        let cp_delete = item.checkpoint_id.clone();
        let sections = checkpoint_summary_lines(&item.payload_json);

        let mut body = div().flex().flex_col().gap(px(6.0));
        if sections.is_empty() {
            let raw: String = item.payload_json.chars().take(600).collect();
            body = body.child(
                div()
                    .p(px(10.0))
                    .rounded(px(7.0))
                    .bg(theme.surface)
                    .text_size(sp(12.0))
                    .text_color(theme.text_secondary)
                    .child(SharedString::from(raw)),
            );
        } else {
            for (heading, items) in sections {
                let mut group = div()
                    .p(px(8.0))
                    .rounded(px(7.0))
                    .bg(theme.surface)
                    .flex()
                    .flex_col()
                    .gap(px(3.0))
                    .child(
                        div()
                            .text_size(sp(11.5))
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.text_tertiary)
                            .child(SharedString::from(heading)),
                    );
                for line in items.into_iter().take(6) {
                    group = group.child(
                        div()
                            .text_size(sp(12.5))
                            .text_color(theme.text_secondary)
                            .child(SharedString::from(format!("• {line}"))),
                    );
                }
                body = body.child(group);
            }
        }

        div()
            .p(px(14.0))
            .rounded(px(10.0))
            .border_1()
            .border_color(theme.border)
            .bg(theme.raised)
            .flex()
            .flex_col()
            .gap(px(8.0))
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap(px(8.0))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .flex_wrap()
                            .gap(px(6.0))
                            .child(
                                div()
                                    .text_size(sp(12.5))
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(theme.text)
                                    .child(SharedString::from(
                                        item.checkpoint_id.chars().take(16).collect::<String>(),
                                    )),
                            )
                            .child(
                                div()
                                    .px(px(6.0))
                                    .py(px(2.0))
                                    .rounded(px(5.0))
                                    .border_1()
                                    .border_color(theme.border)
                                    .text_size(sp(11.5))
                                    .text_color(theme.text_secondary)
                                    .child(SharedString::from(project_display_label(
                                        &item.project_path,
                                    ))),
                            )
                            .children(self.render_session_jump_badge(
                                format!("mem-cp-sess-{idx}"),
                                &item.session_id,
                                item.session_title.as_deref(),
                                theme,
                                cx,
                            )),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(px(8.0))
                            .child(
                                div()
                                    .text_size(sp(11.5))
                                    .text_color(theme.text_tertiary)
                                    .child(SharedString::from(format_memory_timestamp(
                                        item.created_at,
                                    ))),
                            )
                            .child(
                                div()
                                    .id(SharedString::from(format!("mem-cp-delete-{idx}")))
                                    .h(px(24.0))
                                    .px(px(8.0))
                                    .rounded(px(6.0))
                                    .border_1()
                                    .border_color(theme.border)
                                    .bg(theme.surface)
                                    .flex()
                                    .items_center()
                                    .gap(px(4.0))
                                    .cursor_default()
                                    .text_size(sp(11.5))
                                    .text_color(theme.danger)
                                    .hover(|el| el.bg(theme.overlay))
                                    .child(icon("icons/trash.svg", 11.0, theme.danger))
                                    .child(tr!("memory.action_delete"))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.delete_memory_entry(
                                            MemoryEntryKind::Checkpoint,
                                            cp_delete.clone(),
                                            None,
                                            None,
                                            cx,
                                        );
                                    })),
                            ),
                    ),
            )
            .child(body)
    }
}
