use crate::executor::{Executor, QueryResult, TableView};
use eframe::{App, egui};
use egui::Color32;
use egui_extras::syntax_highlighting::CodeTheme;
use tokio::sync::mpsc::{Receiver, Sender, channel};

struct BackendRequest {
    query: String,
    table_name: String,
}

struct BackendResponse {
    result: QueryResult,
    table_view: Option<TableView>,
    table_error: Option<String>,
}

pub struct Application {
    query: String,
    table_name: String,
    result: Option<QueryResult>,
    table_view: Option<TableView>,
    table_error: Option<String>,
    sender: Sender<BackendRequest>,
    receiver: Receiver<BackendResponse>,
}

impl App for Application {
    fn update(&mut self, _ctx: &eframe::egui::Context, _frame: &mut eframe::Frame) {
        eframe::egui::CentralPanel::default().show(_ctx, |ui| {
            let max_rect = ui.max_rect();
            let status_height = 140.0;
            let top_height = (max_rect.height() - status_height).max(120.0);

            if let Ok(response) = self.receiver.try_recv() {
                self.result = Some(response.result);
                self.table_view = response.table_view;
                self.table_error = response.table_error;
            }

            ui.vertical(|ui| {
                ui.allocate_ui(egui::vec2(max_rect.width(), top_height), |ui| {
                    ui.columns(2, |columns| {
                        self.draw_query_pane(top_height, &mut columns[0]);
                        self.draw_table_pane(&mut columns[1]);
                    });
                });

                ui.separator();

                ui.allocate_ui(
                    egui::vec2(max_rect.width(), status_height),
                    |ui| self.draw_status_pane(ui),
                );
            });
        });
    }
}

impl Application {
    pub fn new() -> Self {
        let ch1 = channel(100); // Query channel
        let ch2 = channel(100); // Result channel
        let mut thread = BackendThread::new(ch2.0, ch1.1);
        tokio::spawn(async move {
            thread.run().await;
        });
        Self {
            query: String::new(),
            table_name: String::new(),
            result: None,
            table_view: None,
            table_error: None,
            sender: ch1.0,
            receiver: ch2.1,
        }
    }

    pub fn launch(self) {
        let options = eframe::NativeOptions::default();
        let _ = eframe::run_native("SQuirreL GUI", options, Box::new(|_cc| Ok(Box::new(self))));
    }

    fn draw_code_editor(&mut self, height: f32, ui: &mut egui::Ui) {
        let mut layouter = |ui: &egui::Ui, buf: &dyn egui::TextBuffer, wrap_width: f32| {
            let mut layout_job = egui_extras::syntax_highlighting::highlight(
                ui.ctx(),
                ui.style(),
                &CodeTheme::dark(20.0),
                buf.as_str(),
                "SQL",
            );
            layout_job.wrap.max_width = wrap_width;
            ui.fonts_mut(|f| f.layout_job(layout_job))
        };
        egui::ScrollArea::vertical()
            .min_scrolled_height(height)
            .show(ui, |ui| {
                ui.take_available_height();
                let editor = egui::TextEdit::multiline(&mut self.query)
                    .font(egui::TextStyle::Monospace) // for cursor height
                    .code_editor()
                    .desired_rows(999)
                    .lock_focus(true)
                    .desired_width(f32::INFINITY)
                    .layouter(&mut layouter);
                ui.add(editor);
            });
    }

    fn draw_query_pane(&mut self, height: f32, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.heading("Query Editor");
            ui.separator();

            self.draw_code_editor((height - 70.0).max(80.0), ui);

            ui.add_space(8.0);
            if ui.button("Query!").clicked() {
                let request = BackendRequest {
                    query: self.query.clone(),
                    table_name: self.table_name.trim().to_owned(),
                };
                self.sender.try_send(request).unwrap_or_else(|err| {
                    eprintln!("Failed to send query: {}", err);
                });
            }
        });
    }

    fn draw_table_pane(&mut self, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.heading("Table View");
            ui.horizontal(|ui| {
                ui.label("Table:");
                ui.text_edit_singleline(&mut self.table_name);
            });
            ui.separator();
            self.draw_table(ui);
        });
    }

    fn draw_table(&self, ui: &mut egui::Ui) {
        if self.table_name.trim().is_empty() {
            ui.colored_label(Color32::GRAY, "Enter a table name to display it here.");
            return;
        }

        if let Some(error) = &self.table_error {
            ui.colored_label(Color32::RED, error);
            return;
        }

        let Some(table_view) = self.table_view.as_ref() else {
            ui.colored_label(Color32::GRAY, "Run a query to refresh the selected table.");
            return;
        };

        let rows = &table_view.rows;

        let column_count = table_view.columns.len();
        if column_count == 0 {
            ui.colored_label(Color32::LIGHT_BLUE, "Table does not contain any columns.");
            return;
        }

        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.colored_label(
                Color32::LIGHT_GRAY,
                format!("{} · {} row(s), {} column(s)", table_view.name, rows.len(), column_count),
            );
            if rows.is_empty() {
                ui.colored_label(Color32::LIGHT_BLUE, "This table is currently empty.");
            }
            ui.separator();

            egui::ScrollArea::both()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    egui::Grid::new("query_result_table")
                        .striped(true)
                        .spacing([16.0, 8.0])
                        .show(ui, |ui| {
                            for column_name in &table_view.columns {
                                ui.strong(column_name);
                            }
                            ui.end_row();

                            for row in rows {
                                for column_index in 0..column_count {
                                    let value = row
                                        .get(column_index)
                                        .map(String::as_str)
                                        .unwrap_or("NULL");
                                    ui.monospace(value);
                                }
                                ui.end_row();
                            }
                        });
                });
        });
    }

    fn draw_status_pane(&self, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.heading("Query Status");
            ui.separator();

            match &self.result {
                Some(QueryResult::Rows(rows)) => {
                    let column_count = rows.iter().map(|row| row.len()).max().unwrap_or(0);
                    ui.colored_label(
                        Color32::LIGHT_GREEN,
                        format!("Query returned {} row(s) and {} column(s).", rows.len(), column_count),
                    );
                }
                Some(QueryResult::Success) => {
                    ui.colored_label(Color32::GREEN, "Query executed successfully.");
                }
                Some(QueryResult::Error(msg)) => {
                    ui.colored_label(Color32::RED, format!("Error: {}", msg));
                }
                None => {
                    ui.colored_label(Color32::GRAY, "No query executed yet.");
                }
            }

            if let Some(table_view) = &self.table_view {
                ui.add_space(4.0);
                ui.colored_label(
                    Color32::LIGHT_BLUE,
                    format!(
                        "Watched table '{}' refreshed: {} row(s), {} column(s).",
                        table_view.name,
                        table_view.rows.len(),
                        table_view.columns.len()
                    ),
                );
            } else if let Some(error) = &self.table_error {
                ui.add_space(4.0);
                ui.colored_label(Color32::RED, format!("Table view error: {}", error));
            }

            ui.add_space(8.0);
            ui.label("Latest query:");
            egui::ScrollArea::vertical().max_height(60.0).show(ui, |ui| {
                if self.query.trim().is_empty() {
                    ui.colored_label(Color32::DARK_GRAY, "(empty)");
                } else {
                    ui.monospace(self.query.trim());
                }
            });
        });
    }
}

struct BackendThread {
    sender: Sender<BackendResponse>,
    receiver: Receiver<BackendRequest>,
    executor: Executor,
}

impl BackendThread {
    pub fn new(sender: Sender<BackendResponse>, receiver: Receiver<BackendRequest>) -> Self {
        Self {
            sender,
            receiver,
            executor: Executor::new(),
        }
    }

    pub async fn run(&mut self) {
        while let Some(request) = self.receiver.recv().await {
            println!("Received query: {}", request.query);
            let result = self.executor.run(request.query).await;
            let (table_view, table_error) = if request.table_name.trim().is_empty() {
                (None, None)
            } else {
                match self.executor.load_table(request.table_name.trim()).await {
                    Ok(table_view) => (Some(table_view), None),
                    Err(err) => (None, Some(err)),
                }
            };
            let response = BackendResponse {
                result,
                table_view,
                table_error,
            };
            if let Err(err) = self.sender.send(response).await {
                eprintln!("Failed to send query result: {}", err);
            }
        }
    }
}
