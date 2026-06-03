//! gui — the graphical chat console (the default `peptide` mode): a native window using
//! the system webview (wry) — WKWebView on macOS, WebView2/Edge on Windows, WebKitGTK on
//! Linux. The whole UI is HTML/CSS/JS (src/peptide_ui.html, Claude dark theme); this file
//! is the glue: boot the engine, stream replies into the page, send the page's commands to
//! the socket, and drive the staged reconnect/boot flow when the connection is lost.

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use tao::dpi::LogicalSize;
use tao::event::{Event, StartCause, WindowEvent};
use tao::event_loop::{ControlFlow, EventLoopBuilder, EventLoopProxy};

#[cfg(target_os = "linux")]
use tao::platform::unix::WindowExtUnix;

use tao::window::WindowBuilder;
use wry::http::Request;
use wry::WebViewBuilder;

#[cfg(target_os = "linux")]
use wry::WebViewBuilderExtUnix;

use crate::debug_target::DebugTarget; // feature surface: match_status / char_icon
use crate::interpreter::{split_commands, translate, Translated};

/// Events pushed into the loop from worker threads, forwarded to the page.
enum Ev {
    Line(String), // an engine reply line -> onLine(...)
    Js(String),   // arbitrary JS to evaluate (status, modal, reconnect/boot callbacks)
}

type SharedWriter = Arc<Mutex<Option<TcpStream>>>;
type SharedCleanup = Arc<Mutex<Option<crate::ui::Cleanup>>>;
type SharedConn = Arc<Mutex<(u16, String)>>; // the live session's port + auth token

// IPC control messages (everything else is a user command -> translate).
const RECONNECT: &str = "@@reconnect";     // TCP back into the current session
const BOOT_QUICK: &str = "@@boot:quick";   // launch a fresh engine, auto-spawn for testing
const BOOT_REGULAR: &str = "@@boot:regular"; // launch a fresh engine, no auto-spawn
const BOOT_SSF2: &str = "@@boot:ssf2";     // launch the patched SSF2 app + drive the same console
const DISCONNECT: &str = "@@disconnect";   // tear down the current engine (status-button menu)

/// Which engine the live console is attached to. The whole console — commands,
/// the matchStatus poll, replies — routes by this so "Launch Peptide" drives
/// Fraymakers OR SSF2 through the identical UI. 0 = Fraymakers, 1 = SSF2.
static GUI_ENGINE: AtomicU8 = AtomicU8::new(0);

fn engine_is_ssf2() -> bool { GUI_ENGINE.load(Ordering::Relaxed) == 1 }
// Screen-router verbs (Home / Setup / Converter / FrayTools Hook). Heavy work
// (conversion, FrayTools CDP, file dialogs) runs on a worker thread and posts
// results back via Ev::Js — never block the tao event loop.
const PICK_DIR_PREFIX: &str = "@@pick:dir:";       // @@pick:dir:<replyFn>
const PICK_FILE_PREFIX: &str = "@@pick:file:";     // @@pick:file:<replyFn>[:<ext>]
const SETUP_SAVE_PREFIX: &str = "@@setup:save:";   // @@setup:save:<json>
const SETUP_RESET: &str = "@@setup:reset";         // clear config -> reopen first-run wizard
const CONVERT_PREFIX: &str = "@@convert:start:";   // @@convert:start:<json>
const PUBLISH_PREFIX: &str = "@@publish:add:";      // @@publish:add:<json {char, output}>
const PROJECTS_LIST: &str = "@@projects:list";      // enumerate .fraytools projects (launch modal)
const FRAY_PREFIX: &str = "@@fray:";                // @@fray:export|render|harness:<json>

pub fn launch() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    apply_default_linux_env_vars();
 
    let event_loop = EventLoopBuilder::<Ev>::with_user_event().build();
    let window = WindowBuilder::new()
        .with_title("Peptide")
        .with_inner_size(LogicalSize::new(940.0, 720.0))
        .with_min_inner_size(LogicalSize::new(520.0, 400.0))
        .build(&event_loop)
        .map_err(|e| io(&e.to_string()))?;

    // No eager engine boot: the window opens to Setup (first run) or Home, and the
    // engine is launched lazily only when the user picks "Launch Peptide". The
    // shared engine handles start empty and are filled by boot_new/reconnect.
    let writer: SharedWriter = Arc::new(Mutex::new(None));
    let cleanup: SharedCleanup = Arc::new(Mutex::new(None));
    let conn: SharedConn = Arc::new(Mutex::new((0, String::new())));

    // matchStatus feed: poll the engine ~5×/s while connected. The reply (E:MATCHSTATUS:…)
    // is routed to the status widgets by spawn_reader, NOT the chat. Host-driven so the
    // engine needs no per-frame bytecode (commands.hsx::matchStatus does the gathering).
    {
        let poll_writer = writer.clone();
        let poll_cleanup = cleanup.clone();
        let poll_proxy = event_loop.create_proxy();
        thread::spawn(move || {
            let mut ssf2_misses = 0u32; // consecutive PING failures → SSF2 has gone away
            loop {
            thread::sleep(Duration::from_millis(200));
            if engine_is_ssf2() {
                // SSF2: first a cheap PING — this is BOTH the liveness check (so we
                // notice when SSF2 is closed/crashes; there's no socket EOF like
                // Fraymakers) and the gate for the heavier matchStatus read.
                let alive = crate::ssf2_bridge::request("PING", Duration::from_millis(700))
                    .map(|r| r == "pong").unwrap_or(false);
                if !alive {
                    ssf2_misses += 1;
                    if ssf2_misses >= 5 {
                        // SSF2 stopped answering — treat it as gone: tear down + tell
                        // the page (no auto-reconnect; SSF2 has no reconnect path).
                        ssf2_misses = 0;
                        GUI_ENGINE.store(0, Ordering::Relaxed);
                        if let Some(mut c) = poll_cleanup.lock().ok().and_then(|mut g| g.take()) { c.dispose(); }
                        let _ = poll_proxy.send_event(Ev::Js(
                            "window.onSsf2Closed && onSsf2Closed()".into()));
                    }
                    continue;
                }
                ssf2_misses = 0;
                // pull the matchStatus feed through the SAME trait surface
                // (DebugTarget::match_status) and route it to the widget. Whenever a
                // match exists — however it was started — the widget populates.
                let mut target = crate::ssf2_target::Ssf2Target::new();
                if let Ok(Some(line)) = target.match_status() {
                    route_ssf2_line(&line, &poll_proxy);
                }
            } else if let Ok(mut g) = poll_writer.lock() {
                // Fraymakers: poll over the live socket (engine runs commands.hsx::matchStatus).
                if let Some(s) = g.as_mut() {
                    if s.write_all(b"e matchStatus()\n").and_then(|_| s.flush()).is_err() {
                        // socket dropped — leave it; boot_new/reconnect installs a fresh one
                    }
                }
            }
            }
        });
    }

    let char_name = crate::config::Config::load().char_name();
    let init = format!("window.__CHAR__={};", js_str(&char_name));

    let ipc_writer = writer.clone();
    let ipc_cleanup = cleanup.clone();
    let ipc_conn = conn.clone();
    let ipc_proxy = event_loop.create_proxy();
    let ipc_char = char_name.clone();
    // Read the UI from disk at launch (no compiled-in copy); wry's with_html
    // borrows a &str, so the String must outlive the builder.
    let ui_html = crate::read_asset("peptide_ui.html");

    let webview_builder = WebViewBuilder::new()
        .with_html(&ui_html)
        .with_initialization_script(&init)
        .with_ipc_handler(move |req: Request<String>| {
            let body = req.body().to_string();
            let (w, cl, cn, px, ch) = (ipc_writer.clone(), ipc_cleanup.clone(), ipc_conn.clone(),
                                       ipc_proxy.clone(), ipc_char.clone());
            let b = body.as_str();
            if b == RECONNECT {
                thread::spawn(move || reconnect_existing(w, cn, px, ch));
            } else if b.starts_with(BOOT_QUICK) {
                // @@boot:quick[:<char>] — the trailing char (the picked project) is
                // baked as the launch default; absent -> fall back to the config char.
                let chosen = boot_char(b, BOOT_QUICK).unwrap_or(ch);
                thread::spawn(move || boot_new(w, cl, cn, px, chosen, true));
            } else if b.starts_with(BOOT_REGULAR) {
                let chosen = boot_char(b, BOOT_REGULAR).unwrap_or(ch);
                thread::spawn(move || boot_new(w, cl, cn, px, chosen, false));
            } else if b.starts_with(BOOT_SSF2) {
                // @@boot:ssf2[:<char>] — patch + launch SSF2 and attach the console to it.
                // With a character: fast-boot (auto-spawn it). Without: a normal boot to
                // the SSF2 menu (no spawn) — the user drives it from the console.
                let chosen = boot_char(b, BOOT_SSF2);
                thread::spawn(move || boot_ssf2(cl, px, chosen));
            } else if b == DISCONNECT {
                thread::spawn(move || disconnect_engine(w, cl, px));
            } else if let Some(slot) = b.strip_prefix("@@icon:") {
                // A character's stock icon for match slot <slot>, via the SAME feature
                // surface (DebugTarget::char_icon). Fraymakers rips the icon over its
                // socket; SSF2 reports no icon (capability gap) so the widget keeps its
                // glyph. Only the transport differs — the feature is one trait method.
                if let Ok(n) = slot.trim().parse::<u32>() {
                    if engine_is_ssf2() {
                        let mut target = crate::ssf2_target::Ssf2Target::new();
                        if let Ok(Some(line)) = target.char_icon(n) { route_ssf2_line(&line, &ipc_proxy); }
                    } else {
                        handle_command(&format!("e iconFeed({n})"), &ipc_writer, &ipc_proxy);
                    }
                }
            } else if b.starts_with("@@") {
                handle_screen_verb(b, &ipc_proxy);
            } else {
                handle_command(&body, &ipc_writer, &ipc_proxy);
            }
        });
    
    #[cfg(not(target_os = "linux"))]
    let webview = webview_builder
        .build(&window)
        .map_err(|e| io(&e.to_string()))?;

    #[cfg(target_os = "linux")]
    let webview = {
        use gtk::traits::WidgetExt;
        // Note that for linux targets we need to initialize gtk before building the webview
        gtk::init().map_err(|e| io(&e.to_string()))?;
        // Likewise for display we have to build using an instance of gtk::Box, here we use the default
        if let Some(vbox) = window.default_vbox() {
            vbox.show_all();
            webview_builder
                .build_gtk(vbox)
                .map_err(|e| io(&e.to_string()))?
        } else {
            use gtk::traits::ContainerExt;
            // We create our own vbox instance if the default vbox isn't present
            let vbox = gtk::Box::new(gtk::Orientation::Vertical, 0);
            window.gtk_window().add(&vbox);
            vbox.show_all();
            webview_builder
                .build_gtk(&vbox)
                .map_err(|e| io(&e.to_string()))?
        }
    };

    event_loop.run(move |event, _t, control_flow| {
        *control_flow = ControlFlow::Wait;
        match event {
            Event::NewEvents(StartCause::Init) => {
                // Route the page to Setup or Home based on the persisted config.
                let _ = webview.evaluate_script(&format!(
                    "window.bootRoute && bootRoute({})", config_json()));
            }
            Event::UserEvent(Ev::Line(line)) => {
                let _ = webview.evaluate_script(&format!("window.onLine && onLine({})", js_str(&line)));
            }
            Event::UserEvent(Ev::Js(js)) => {
                let _ = webview.evaluate_script(&js);
            }
            Event::WindowEvent { event: WindowEvent::CloseRequested, .. } => {
                if let Some(mut c) = cleanup.lock().ok().and_then(|mut g| g.take()) {
                    c.dispose();
                }
                *control_flow = ControlFlow::Exit;
            }
            _ => {}
        }
    });
}

fn io(msg: &str) -> std::io::Error {
    std::io::Error::other(msg.to_string())
}

/// Parse the optional `:<char>` suffix off a boot verb (`@@boot:quick:mario` ->
/// `Some("mario")`, `@@boot:quick` -> `None`).
fn boot_char(verb: &str, prefix: &str) -> Option<String> {
    verb.strip_prefix(prefix)
        .and_then(|r| r.strip_prefix(':'))
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// A small JSON blob describing the current config + setup state, passed to the
/// page's `bootRoute` so it can open the Setup wizard (first run / incomplete) or
/// Home. Carries autodetect results so the wizard can pre-fill + badge the paths.
fn config_json() -> String {
    let cfg = crate::config::Config::load();
    let s = |v: &str| js_str(v);
    let frayroot = cfg.fraymakers_root().map(|p| p.display().to_string()).unwrap_or_default();
    let frayexe = cfg.fraytools_exe().map(|p| p.display().to_string()).unwrap_or_default();
    // Whether FrayTools resolves to a file that actually exists on disk (not just a
    // configured-but-missing path). The "isn't built yet" modal uses this to decide
    // between offering an inline Export and sending the user to Setup.
    let fraytools_ready = cfg.fraytools_exe().map(|p| p.is_file()).unwrap_or(false);
    // Autodetect for the wizard: did we find each tool on disk? And the path to
    // pre-fill FrayTools with when the user hasn't set one yet.
    let fraymakers_detected = crate::platform::fraymakers_root().is_some();
    let fraytools_detected = crate::platform::detected_fraytools_path().is_some();
    let fraytools_val = if !cfg.fraytools_path.is_empty() {
        cfg.fraytools_path.clone()
    } else {
        crate::platform::detected_fraytools_path().map(|p| p.display().to_string()).unwrap_or_default()
    };
    // SSF2 — the second engine "Launch Peptide" can drive. Pre-fill with the
    // configured path, else the autodetected install; flag whether one was found
    // so the wizard can show a "detected" badge like FrayTools/Fraymakers.
    let ssf2_detected = crate::platform::detected_ssf2_path().is_some();
    let ssf2_val = if !cfg.ssf2_app.is_empty() {
        cfg.ssf2_app.clone()
    } else {
        crate::platform::detected_ssf2_path().map(|p| p.display().to_string()).unwrap_or_default()
    };
    // SSF2 is usable for Launch if the resolved path exists on disk.
    let ssf2_ready = cfg.ssf2_app().map(|p| p.exists()).unwrap_or(false);
    format!(
        "{{\"configured\":{},\"setupComplete\":{},\"currentChar\":{},\"stage\":{},\"assist\":{},\
          \"fraymakersRoot\":{},\
          \"fraymakersDetected\":{},\"fraytoolsPath\":{},\"fraytoolsDetected\":{},\
          \"fraytoolsExe\":{},\"fraytoolsReady\":{},\
          \"ssf2App\":{},\"ssf2Detected\":{},\"ssf2Ready\":{},\
          \"outputDir\":{},\"miscSsf\":{}}}",
        cfg.configured,
        cfg.setup_complete(),
        s(&cfg.char_name()),
        s(&cfg.stage()),
        s(&cfg.assist()),
        s(&frayroot),
        fraymakers_detected,
        s(&fraytools_val),
        fraytools_detected,
        s(&frayexe),
        fraytools_ready,
        s(&ssf2_val),
        ssf2_detected,
        ssf2_ready,
        s(&cfg.output_dir().display().to_string()),
        s(&cfg.misc_ssf),
    )
}

/// Dispatch a screen-router control message. Everything that blocks (file
/// dialogs, conversion, FrayTools CDP) runs on a worker thread that posts results
/// back via Ev::Js — the tao event loop must never block.
fn handle_screen_verb(verb: &str, proxy: &EventLoopProxy<Ev>) {
    if let Some(rest) = verb.strip_prefix(PICK_DIR_PREFIX) {
        let (reply, px) = (rest.to_string(), proxy.clone());
        thread::spawn(move || pick_path(true, &reply, "", &px));
    } else if let Some(rest) = verb.strip_prefix(PICK_FILE_PREFIX) {
        // <replyFn>[:<ext>]
        let mut it = rest.splitn(2, ':');
        let reply = it.next().unwrap_or("").to_string();
        let ext = it.next().unwrap_or("").to_string();
        let px = proxy.clone();
        thread::spawn(move || pick_path(false, &reply, &ext, &px));
    } else if let Some(json) = verb.strip_prefix(SETUP_SAVE_PREFIX) {
        let (json, px) = (json.to_string(), proxy.clone());
        thread::spawn(move || save_setup(&json, &px));
    } else if verb == SETUP_RESET {
        let px = proxy.clone();
        thread::spawn(move || reset_setup(&px));
    } else if verb == PROJECTS_LIST {
        let px = proxy.clone();
        thread::spawn(move || list_projects(&px));
    } else if let Some(json) = verb.strip_prefix(CONVERT_PREFIX) {
        let (json, px) = (json.to_string(), proxy.clone());
        thread::spawn(move || run_convert(&json, &px));
    } else if let Some(json) = verb.strip_prefix(PUBLISH_PREFIX) {
        let (json, px) = (json.to_string(), proxy.clone());
        thread::spawn(move || add_publish_folder(&json, &px));
    } else if let Some(rest) = verb.strip_prefix(FRAY_PREFIX) {
        // export:<json> | render:<json> | harness:<json>
        let (rest, px) = (rest.to_string(), proxy.clone());
        thread::spawn(move || run_fraytools(&rest, &px));
    }
    // Unknown @@verbs are ignored (forward-compat with the page).
}

/// Minimal JSON-string-field extractor for the small, flat objects the page
/// sends (`{"k":"v",...}`). Avoids pulling serde_json's Value into the hot path
/// and is enough for our known keys. Returns the unescaped string value for `key`.
fn json_str_field(json: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\"");
    let i = json.find(&needle)? + needle.len();
    let rest = &json[i..];
    let colon = rest.find(':')?;
    let after = rest[colon + 1..].trim_start();
    let mut chars = after.chars();
    if chars.next()? != '"' { return None; }
    let mut out = String::new();
    let mut esc = false;
    for c in chars {
        if esc { out.push(c); esc = false; continue; }
        match c {
            '\\' => esc = true,
            '"' => return Some(out),
            _ => out.push(c),
        }
    }
    None
}

/// The configured FrayTools project folder (`output_dir`) as an absolute, existing
/// directory — for defaulting file pickers there. `None` if it doesn't exist.
fn resolved_project_dir() -> Option<std::path::PathBuf> {
    let d = crate::config::Config::load().output_dir();
    let abs = if d.is_absolute() { d } else { std::env::current_dir().ok()?.join(d) };
    abs.is_dir().then_some(abs)
}

/// Resolve the `.fraytools` project FILE for a converted character, so the
/// "isn't built yet" modal can publish it inline. Looks for the conversion's
/// `<output>/<char>/<char>.fraytools`, then a top-level `<output>/<char>.fraytools`.
/// `None` when no project exists (then we fall back to the generic boot-failed prompt).
fn project_file_for(char_name: &str) -> Option<String> {
    let dir = crate::config::Config::load().output_dir();
    if let Some(p) = crate::platform::find_project_file(&dir.join(char_name)) {
        return Some(p.display().to_string());
    }
    let top = dir.join(format!("{char_name}.fraytools"));
    top.is_file().then(|| top.display().to_string())
}

/// Open a native folder/file picker (rfd) and call back the named JS function with
/// the chosen path (or empty string if cancelled).
fn pick_path(dir: bool, reply_fn: &str, ext: &str, proxy: &EventLoopProxy<Ev>) {
    let chosen = if dir {
        rfd::FileDialog::new().pick_folder()
    } else {
        let mut d = rfd::FileDialog::new();
        // `ext` may be a comma-separated list (e.g. "ssf,swf,dat") so a single
        // picker can accept every supported input type, matching what the
        // converter reads. Empty = no filter (any file).
        let exts: Vec<&str> = ext.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        if !exts.is_empty() { d = d.add_filter(exts.join("/"), &exts); }
        // A `.fraytools` pick (launch / FrayTools Hook) opens in the configured
        // project folder by default, since that's where projects live.
        if ext == "fraytools" {
            if let Some(start) = resolved_project_dir() {
                d = d.set_directory(start);
            }
        }
        d.pick_file()
    };
    let path = chosen.map(|p| p.display().to_string()).unwrap_or_default();
    // Guard the reply-fn name to a safe identifier so we never inject arbitrary JS.
    if reply_fn.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !reply_fn.is_empty() {
        let _ = proxy.send_event(Ev::Js(format!(
            "window.{f} && {f}({p})", f = reply_fn, p = js_str(&path))));
    }
}

/// Persist the setup wizard form (`{fraymakersRoot, fraytoolsPath, outputDir,
/// miscSsf}`) and report back whether setup is now complete. Marks the config as
/// `configured` so the first-run wizard doesn't reappear. The character is no
/// longer a setup field — it's chosen at launch by picking a `.fraytools` project.
fn save_setup(json: &str, proxy: &EventLoopProxy<Ev>) {
    let mut cfg = crate::config::Config::load();
    if let Some(v) = json_str_field(json, "fraymakersRoot") { cfg.fraymakers_root = v; }
    if let Some(v) = json_str_field(json, "fraytoolsPath")  { cfg.fraytools_path = v; }
    if let Some(v) = json_str_field(json, "ssf2App")        { cfg.ssf2_app = v; }
    if let Some(v) = json_str_field(json, "outputDir")      { cfg.output_dir = v; }
    if let Some(v) = json_str_field(json, "miscSsf")        { cfg.misc_ssf = v; }
    cfg.configured = true;
    cfg.save();
    let _ = proxy.send_event(Ev::Js(format!(
        "window.onSetupSaved && onSetupSaved({})", config_json())));
}

/// Clear the persisted config and re-route the page to the first-run wizard with
/// freshly autodetected defaults (the Setup screen's "Reset to defaults" button).
fn reset_setup(proxy: &EventLoopProxy<Ev>) {
    crate::config::Config::reset();
    let _ = proxy.send_event(Ev::Js(format!(
        "window.bootRoute && bootRoute({})", config_json())));
}

/// Enumerate the converted `.fraytools` projects in the output dir for the launch
/// modal: each `<output>/<char>/<char>.fraytools` (and any top-level `.fraytools`).
/// Posts `onProjects([{char, path}, …])` back to the page.
fn list_projects(proxy: &EventLoopProxy<Ev>) {
    let dir = crate::config::Config::load().output_dir();
    let mut items: Vec<(String, String)> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            let proj = if p.is_dir() {
                crate::platform::find_project_file(&p)
            } else if p.extension().map(|x| x == "fraytools").unwrap_or(false) {
                Some(p.clone())
            } else {
                None
            };
            if let Some(proj) = proj {
                if let Some(stem) = proj.file_stem().map(|s| s.to_string_lossy().into_owned()) {
                    // Show the folder that holds the .fraytools, relative as configured
                    // (e.g. "./characters/mario"), not the full path to the file.
                    let folder = proj.parent().unwrap_or(&proj).display().to_string();
                    if !stem.is_empty() { items.push((stem, folder)); }
                }
            }
        }
    }
    items.sort();
    items.dedup();
    let mut json = String::from("[");
    for (i, (c, path)) in items.iter().enumerate() {
        if i > 0 { json.push(','); }
        json.push_str(&format!("{{\"char\":{},\"path\":{}}}", js_str(c), js_str(path)));
    }
    json.push(']');
    let _ = proxy.send_event(Ev::Js(format!("window.onProjects && onProjects({json})")));
}

/// Run an in-process conversion (`{input, output?, name?, miscSsf?}`) on this
/// worker thread, reporting progress + the result back to the convert screen.
fn run_convert(json: &str, proxy: &EventLoopProxy<Ev>) {
    use ssf2_converter::{run_conversion, ConvertOptions};
    let Some(input) = json_str_field(json, "input").filter(|s| !s.is_empty()) else {
        let _ = proxy.send_event(Ev::Js(
            "window.onConvertError && onConvertError(\"no input file selected\")".into()));
        return;
    };
    let cfg = crate::config::Config::load();
    let mut opts = ConvertOptions::new(&input);
    opts.output = json_str_field(json, "output").filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from).unwrap_or_else(|| cfg.output_dir());
    opts.name = json_str_field(json, "name").filter(|s| !s.is_empty());
    opts.misc_ssf = json_str_field(json, "miscSsf").filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .or_else(|| if cfg.misc_ssf.is_empty() { None } else { Some(std::path::PathBuf::from(&cfg.misc_ssf)) });

    let _ = proxy.send_event(Ev::Js(format!(
        "window.onConvertProgress && onConvertProgress({})",
        js_str(&format!("Converting {}…", input)))));

    match run_conversion(opts) {
        Ok(summary) => {
            // Build a JSON result: project dir, chars, .fraytools files, warnings.
            let files: Vec<String> = summary.fraytools_files.iter()
                .map(|p| p.display().to_string()).collect();
            let result = format!(
                "{{\"projectDir\":{},\"characters\":{},\"files\":{},\"warnings\":{}}}",
                js_str(&summary.project_dir.display().to_string()),
                js_array(&summary.characters),
                js_array(&files),
                js_array(&summary.warnings),
            );
            let _ = proxy.send_event(Ev::Js(format!(
                "window.onConvertDone && onConvertDone({result})")));
        }
        Err(e) => {
            let _ = proxy.send_event(Ev::Js(format!(
                "window.onConvertError && onConvertError({})", js_str(&e.to_string()))));
        }
    }
}

/// Add the Fraymakers `custom/<Char>` folder to a converted character's
/// `.fraytools` publishFolders (`{char, output}`), reporting the result.
fn add_publish_folder(json: &str, proxy: &EventLoopProxy<Ev>) {
    let cfg = crate::config::Config::load();
    let char_name = json_str_field(json, "char").filter(|s| !s.is_empty())
        .unwrap_or_else(|| cfg.char_name());
    let output = json_str_field(json, "output").filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from).unwrap_or_else(|| cfg.output_dir());
    let char_dir = output.join(&char_name);
    match crate::platform::ensure_fraymakers_publish_folder(&char_name, &char_dir) {
        Ok(rel) => {
            let _ = proxy.send_event(Ev::Js(format!(
                "window.onPublishResult && onPublishResult(true, {})",
                js_str(&format!("Added publish folder: {rel}")))));
        }
        Err(e) => {
            let _ = proxy.send_event(Ev::Js(format!(
                "window.onPublishResult && onPublishResult(false, {})", js_str(&e))));
        }
    }
}

// Sets an environment variable to a given value if unset
//
// * `key` - The environment variable key 
// * `value` - The value to be set to the environment variable
// * `unset_if_empty` - When true, the environment variable is unset if the value is empty
fn set_default_env_var(key: &str, value: &str, unset_if_empty: bool) {
    let env_var = std::env::var_os(key);
    if env_var.is_none() {
        std::env::set_var(key, value);
    } else if let Some(v) = env_var {
        if unset_if_empty && v.is_empty() {
            std::env::remove_var(key);
        }
    }
}

// Setting default environment variables to fix rendering issues on linux
fn apply_default_linux_env_vars() {
    // Using x11 by default, Note that if an empty string is passed to any of the given environment
    // variables, the environment variable would be removed, ensuring that users can properly opt
    // out of our defaults
    set_default_env_var("GDK_BACKEND", "x11", true);
    // Hopefully should deal with rendering issues on nvidia gpus
    set_default_env_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1", true);
    set_default_env_var("WEBKIT_DISABLE_COMPOSITING_MODE", "1", true);
}

/// Drive the FrayTools CDP harness for the Hook screen. `rest` is
/// `export:<json>` | `render:<json>` | `harness:<json>` where json carries the
/// flags (project / entity / animation / frame). Reuses the same fraytools::*
/// entry points the CLI uses, by assembling an argv.
fn run_fraytools(rest: &str, proxy: &EventLoopProxy<Ev>) {
    let mut it = rest.splitn(2, ':');
    let action = it.next().unwrap_or("");
    let json = it.next().unwrap_or("{}");

    // Build an argv: ["peptide", "<action>", "--project", P, "--entity", E, ...].
    let mut argv: Vec<String> = vec!["peptide".into(), action.into()];
    let mut push = |flag: &str, key: &str| {
        if let Some(v) = json_str_field(json, key) {
            if !v.is_empty() { argv.push(flag.into()); argv.push(v); }
        }
    };
    push("--project", "project");
    push("--entity", "entity");
    push("--animation", "animation");
    push("--frame", "frame");
    push("--out", "out");
    push("--fraytools", "fraytools");
    // The page rarely sends explicit tool paths (the inline "Export Now" on the
    // not-built modal sends only the project). Fall back to the Setup-configured
    // locations so we honor a custom install instead of fraytools::export's
    // hardcoded per-OS defaults (which ignore FRAY_DIR / the saved config). Only
    // add each if the page didn't already supply one.
    let cfg = crate::config::Config::load();
    if json_str_field(json, "fraytools").filter(|s| !s.is_empty()).is_none() {
        if let Some(exe) = cfg.fraytools_exe() {
            argv.push("--fraytools".into());
            argv.push(exe.display().to_string());
        }
    }
    if json_str_field(json, "fraymakers-root").filter(|s| !s.is_empty()).is_none() {
        if let Some(root) = cfg.fraymakers_root() {
            argv.push("--fraymakers-root".into());
            argv.push(root.display().to_string());
        }
    }

    let _ = proxy.send_event(Ev::Js(format!(
        "window.onFrayProgress && onFrayProgress({})",
        js_str(&format!("Running FrayTools {action}…")))));

    let res = match action {
        "export" => crate::fraytools::export(&argv),
        "render" => crate::fraytools::render(&argv),
        "harness" => crate::fraytools::harness(&argv),
        _ => Err(anyhow::anyhow!("unknown FrayTools action {action:?}")),
    };
    match res {
        Ok(()) => {
            let _ = proxy.send_event(Ev::Js(format!(
                "window.onFrayResult && onFrayResult(true, {})",
                js_str(&format!("FrayTools {action} finished")))));
        }
        Err(e) => {
            let _ = proxy.send_event(Ev::Js(format!(
                "window.onFrayResult && onFrayResult(false, {})", js_str(&e.to_string()))));
        }
    }
}

/// JSON array of strings for evaluate_script payloads.
fn js_array(items: &[String]) -> String {
    let mut s = String::from("[");
    for (i, it) in items.iter().enumerate() {
        if i > 0 { s.push(','); }
        s.push_str(&js_str(it));
    }
    s.push(']');
    s
}

/// Try to TCP back into the running session: re-bind the last port + token and wait a few
/// seconds for the engine to dial in again (no relaunch). Succeeds only if a Fraymakers is
/// alive and reconnects; otherwise the page advances its reconnect flow.
fn reconnect_existing(writer: SharedWriter, conn: SharedConn, proxy: EventLoopProxy<Ev>, char_name: String) {
    let (port, token) = conn.lock().ok().map(|g| g.clone()).unwrap_or((0, String::new()));
    if port == 0 {
        let _ = proxy.send_event(Ev::Js("window.onReconnectFailed && onReconnectFailed(\"no_session\")".into()));
        return;
    }
    match crate::ui::reawait(port, &token, 4) {
        Some((reader, w)) => {
            if let Ok(mut g) = writer.lock() { *g = Some(w); }
            spawn_reader(reader, proxy.clone());
            // reconnecting to a live match -> don't auto-spawn (the character already exists)
            let _ = proxy.send_event(Ev::Js(format!(
                "window.onReconnected && onReconnected({}, {}, false)", port, js_str(&char_name))));
        }
        None => {
            let _ = proxy.send_event(Ev::Js("window.onReconnectFailed && onReconnectFailed(\"no_session\")".into()));
        }
    }
}

/// Launch a fresh patched engine and wait (bounded) for it to connect. `quick` auto-spawns
/// the test character on the page; regular boots clean for normal play. On failure the page
/// returns to the "Fraymakers doesn't seem to be open" prompt.
fn boot_new(writer: SharedWriter, cleanup: SharedCleanup, conn: SharedConn,
            proxy: EventLoopProxy<Ev>, char_name: String, quick: bool) {
    // switching (back) to Fraymakers: flip the console engine (the SSF2 poll branch
    // is gated on GUI_ENGINE, so this also stops it).
    GUI_ENGINE.store(0, Ordering::Relaxed);
    // tear down whatever was there (dead engine + temp files), then patch + launch anew.
    if let Some(mut c) = cleanup.lock().ok().and_then(|mut g| g.take()) {
        c.dispose();
    }
    if let Ok(mut g) = writer.lock() { *g = None; }

    // Stream the bytecode preflight progress into the boot modal as a real bar.
    let pp = proxy.clone();
    let on_progress = move |done: usize, total: usize, label: &str| {
        let _ = pp.send_event(Ev::Js(format!(
            "window.onPatchProgress && onPatchProgress({}, {}, {})",
            done, total, js_str(label))));
    };
    // Quick boot = HEADLESS fast-boot (skip Title, bake the picked custom char); the page
    // sends `spawn <char>` on READY. Regular boot = full (non-headless) Title/UGC boot that
    // is just a live TCP bridge with no auto-spawn. Both keep active TCP.
    let bake = if quick && !char_name.is_empty() { Some(char_name.as_str()) } else { None };
    match crate::ui::patch_and_launch_with_progress(Some(&on_progress), bake) {
        Ok((port, token, mut guard)) => match crate::ui::reawait(port, &token, 30) {
            Some((reader, w)) => {
                if let Ok(mut g) = writer.lock() { *g = Some(w); }
                if let Ok(mut g) = cleanup.lock() { *g = Some(guard); }
                if let Ok(mut g) = conn.lock() { *g = (port, token); }
                spawn_reader(reader, proxy.clone());
                let _ = proxy.send_event(Ev::Js(format!(
                    "window.onReconnected && onReconnected({}, {}, {})",
                    port, js_str(&char_name), if quick { "true" } else { "false" })));
            }
            None => {
                guard.dispose(); // kill the engine that never dialed in
                // It may have crashed during boot — show the crash modal if it left a log.
                thread::sleep(Duration::from_millis(450));
                match read_crash_log() {
                    Some(log) => { let _ = proxy.send_event(Ev::Js(crash_js(&log, &[]))); }
                    None => { let _ = proxy.send_event(Ev::Js(
                        "window.onBootFailed && onBootFailed(\"Fraymakers didn’t connect.\")".into())); }
                }
            }
        },
        Err(e) => {
            let msg = e.to_string();
            // The character has never been published — its `.fra` doesn't exist yet.
            // This isn't a "Fraymakers isn't open" situation, so route it to a dedicated
            // modal that can publish the project inline (Export Now) and then re-boot,
            // but only when we can locate the `.fraytools` to publish.
            match (msg.contains("isn't built yet"), project_file_for(&char_name)) {
                (true, Some(project)) => {
                    let _ = proxy.send_event(Ev::Js(format!(
                        "window.onNotBuilt && onNotBuilt({}, {})",
                        js_str(&char_name), js_str(&project))));
                }
                _ => {
                    let _ = proxy.send_event(Ev::Js(format!(
                        "window.onBootFailed && onBootFailed({})", js_str(&msg))));
                }
            }
        }
    }
}

/// Boot the patched SSF2 app and attach the live console to it — the SSF2 analogue
/// of `boot_new`. Patches + launches SSF2, waits for the reflection handler, flips
/// the console to SSF2 mode, then auto-spawns the chosen character so the user lands
/// in a live match exactly like the Fraymakers quick-boot. Commands, replies and the
/// matchStatus widget then route through the SSF2 backend (same Command vocabulary).
fn boot_ssf2(cleanup: SharedCleanup, proxy: EventLoopProxy<Ev>, char_name: Option<String>) {
    // tear down whatever engine was running (Fraymakers or a prior SSF2).
    if let Some(mut c) = cleanup.lock().ok().and_then(|mut g| g.take()) { c.dispose(); }

    let pp = proxy.clone();
    let progress = move |done: usize, total: usize, label: &str| {
        let _ = pp.send_event(Ev::Js(format!(
            "window.onPatchProgress && onPatchProgress({}, {}, {})", done, total, js_str(label))));
    };
    progress(1, 4, "patching SSF2…");

    // Bind the loopback server BEFORE launch (the engine dials in from its ctor),
    // patch the app to connect to that port, then launch + accept.
    let port = crate::ssf2_bridge::pick_port();
    crate::ssf2_bridge::disconnect();
    let listener = match crate::ssf2_bridge::bind(port) {
        Ok(l) => l,
        Err(e) => { let _ = proxy.send_event(Ev::Js(format!(
            "window.onSsf2BootFailed && onSsf2BootFailed({})", js_str(&format!("Couldn’t open the SSF2 bridge port: {e}"))))); return; }
    };
    let app = match crate::ssf2::install_patched(port) {
        Ok(a) => a,
        Err(e) => { let _ = proxy.send_event(Ev::Js(format!(
            "window.onSsf2BootFailed && onSsf2BootFailed({})", js_str(&format!("Couldn’t patch SSF2: {e}"))))); return; }
    };
    progress(2, 4, "launching SSF2…");
    let exe = app.join("Contents/MacOS/SSF2");
    let child = match std::process::Command::new(&exe)
        .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn() {
        Ok(c) => c,
        Err(e) => { let _ = proxy.send_event(Ev::Js(format!(
            "window.onSsf2BootFailed && onSsf2BootFailed({})", js_str(&format!("Couldn’t launch SSF2: {e}"))))); return; }
    };
    // accept the engine's dial-in over TCP (bounded so a no-show can't hang the boot).
    if let Err(e) = crate::ssf2_bridge::accept_engine(&listener, 30) {
        let _ = proxy.send_event(Ev::Js(format!(
            "window.onSsf2BootFailed && onSsf2BootFailed({})", js_str(&format!("SSF2 didn’t connect: {e}"))))); return;
    }
    // register the process for window-close teardown (reuses the Cleanup path).
    if let Ok(mut g) = cleanup.lock() { *g = Some(crate::ui::Cleanup::for_engine(child)); }

    // Wait until SSF2 is STABLY responsive (the analogue of Fraymakers' READY) —
    // NOT just the first PING, which the per-frame hook answers mid-boot while the
    // engine is still loading and would crash on an early spawn. wait_ready gates on
    // a streak of consecutive PINGs, so commands fire only after loading completes.
    progress(3, 4, "waiting for SSF2 to finish loading…");
    if !crate::ssf2_bridge::wait_ready(10, Duration::from_secs(40)) {
        let _ = proxy.send_event(Ev::Js(
            "window.onSsf2BootFailed && onSsf2BootFailed(\"SSF2 launched but never settled. Try again.\")".into()));
        return;
    }
    progress(4, 4, "ready");

    let label = char_name.clone().unwrap_or_default();

    // Auto-spawn (if a character was chosen) happens BEFORE flipping GUI_ENGINE to
    // SSF2 — otherwise the matchStatus poll (gated on GUI_ENGINE) would run during
    // the spawn's resource load and its per-frame file IO would contend with SSF2's
    // async loader, making the spawn flaky. So: spawn with the bridge to ourselves,
    // THEN turn the console (and its poll) on.
    if let Some(ch) = char_name.filter(|c| !c.is_empty()) {
        let _ = proxy.send_event(Ev::Line(format!("SYS:Engine ready — spawning {ch}…")));
        let mut target = crate::ssf2_target::Ssf2Target::new();
        match crate::debug_target::run_command(&mut target, &format!("spawn {ch}")) {
            Ok(Some(reply)) => { let _ = proxy.send_event(Ev::Line(reply)); }
            Ok(None) => {}
            Err(e) => { let _ = proxy.send_event(Ev::Line(format!("SYS:ERR:{e}"))); }
        }
    } else {
        let _ = proxy.send_event(Ev::Line("SYS:SSF2 ready — type `spawn <char>` to start a match.".into()));
    }

    // Now flip the console to SSF2 and drop the page into it. autospawn=false: the
    // spawn already happened here, so the page must not fire a READY auto-spawn.
    GUI_ENGINE.store(1, Ordering::Relaxed);
    let _ = proxy.send_event(Ev::Js(format!(
        "window.onReconnected && onReconnected(0, {}, false, \"SSF2\")", js_str(&label))));
}

/// Tear down the live engine on request (the status-button "Disconnect"). Fraymakers
/// gets a clean `x` exit (closes the socket) before the process is killed; SSF2's
/// process is killed via its Cleanup. Resets the console to Fraymakers/disconnected
/// and notifies the page WITHOUT triggering the auto-reconnect flow.
fn disconnect_engine(writer: SharedWriter, cleanup: SharedCleanup, proxy: EventLoopProxy<Ev>) {
    if engine_is_ssf2() {
        crate::ssf2_bridge::disconnect(); // drop the loopback socket
    } else if let Ok(mut g) = writer.lock() {
        if let Some(s) = g.as_mut() { let _ = s.write_all(b"x\n"); let _ = s.flush(); }
        *g = None;
    }
    if let Some(mut c) = cleanup.lock().ok().and_then(|mut g| g.take()) { c.dispose(); }
    GUI_ENGINE.store(0, Ordering::Relaxed);
    let _ = proxy.send_event(Ev::Js("window.onManualDisconnect && onManualDisconnect()".into()));
}

/// Route one SSF2 reply line to the page the same way `spawn_reader` routes the
/// Fraymakers socket: matchStatus → the status widget, everything else → the chat.
/// (SSF2 has no charIcon feed.)
fn route_ssf2_line(line: &str, proxy: &EventLoopProxy<Ev>) {
    if let Some((ch, payload)) = crate::interpreter::channel_payload(line) {
        if ch == "matchStatus" {
            let _ = proxy.send_event(Ev::Js(format!(
                "window.onMatchStatus && onMatchStatus({})", js_str(payload))));
        }
        return; // other channels (charIcon) don't exist on SSF2
    }
    let _ = proxy.send_event(Ev::Line(line.to_string()));
}

/// Read the engine crash log (`<fraymakers>/error.log`) if the engine left one. The boot
/// path deletes it before launch, so its presence after a drop means a real crash. Trimmed
/// to a modal-friendly tail. `None` = no crash log (a clean/transient disconnect).
fn read_crash_log() -> Option<String> {
    let root = crate::config::Config::load().fraymakers_root()?;
    let text = std::fs::read_to_string(root.join("error.log")).ok()?;
    let t = text.trim();
    if t.is_empty() { return None; }
    Some(if t.len() > 1600 { format!("…{}", &t[t.len() - 1600..]) } else { t.to_string() })
}

/// The engine's connection ended. If it left a crash log, surface the crash modal with it;
/// otherwise treat it as a transient drop and let the page run its reconnect flow.
fn engine_gone(proxy: &EventLoopProxy<Ev>, resdiag: &[String]) {
    // give the engine a moment to flush error.log after the socket closes
    thread::sleep(Duration::from_millis(450));
    match read_crash_log() {
        Some(log) => { let _ = proxy.send_event(Ev::Js(crash_js(&log, resdiag))); }
        None => { let _ = proxy.send_event(Ev::Js("window.onDisconnected && onDisconnected()".into())); }
    }
}

/// Build the `onCrash(rawEngineLog, enhancedLog)` call. `enhancedLog` is the host-side
/// Enhanced-log text (interpreter::interpret_crash), built from the intact engine log PLUS
/// the engine's `RESDIAG:` breadcrumbs (the failing resource id). The two views are kept
/// separate on the page: Enhanced (this) vs Engine log (the raw error.log).
fn crash_js(log: &str, resdiag: &[String]) -> String {
    let enhanced = crate::interpreter::interpret_crash(log, resdiag).unwrap_or_default();
    format!("window.onCrash && onCrash({}, {})", js_str(log), js_str(&enhanced))
}

/// Socket bytes -> lines -> event loop (-> page). Routes channel feeds (matchStatus/charIcon)
/// to their widgets and filters per-frame ANIM telemetry out of the chat (it's in the widget).
/// On EOF/error the engine connection is gone -> crash modal (if it crashed) or reconnect.
fn spawn_reader(mut reader: BufReader<TcpStream>, proxy: EventLoopProxy<Ev>) {
    thread::spawn(move || {
        let mut buf: Vec<u8> = Vec::with_capacity(256);
        let mut one = [0u8; 1];
        // RESDIAG breadcrumbs go to Peptide's Enhanced (advanced) log, NOT the engine chat —
        // captured here and handed to the crash diagnosis when the engine goes away.
        let mut resdiag: Vec<String> = Vec::new();
        loop {
            match reader.read(&mut one) {
                Ok(0) => { engine_gone(&proxy, &resdiag); break; }
                Ok(_) => {
                    if one[0] == b'\n' {
                        let line = String::from_utf8_lossy(&buf).trim_end_matches('\r').to_string();
                        buf.clear();
                        if line.contains("RESDIAG:") { resdiag.push(line); continue; } // enhanced log, not chat
                        // Channel feeds (matchStatus, charIcon, …) route to their widget, not the chat.
                        if let Some((ch, payload)) = crate::interpreter::channel_payload(&line) {
                            match ch {
                                "charIcon" => {
                                    // payload = "<slot>:<png-hex>;<palette>" -> recolor + data: URL.
                                    if let Some((slot, rest)) = payload.split_once(':') {
                                        let (hex, palette) = rest.split_once(';').unwrap_or((rest, ""));
                                        if let Some(url) = icon_data_url(hex, palette) {
                                            let _ = proxy.send_event(Ev::Js(format!(
                                                "window.onIcon && onIcon({}, {})", js_str(slot), js_str(&url))));
                                        }
                                    }
                                }
                                _ => {
                                    let _ = proxy.send_event(Ev::Js(format!(
                                        "window.onMatchStatus && onMatchStatus({})", js_str(payload))));
                                }
                            }
                            continue;
                        }
                        // Per-transition animation + damage telemetry is shown live in the
                        // matchStatus widgets (current animation and % per character), so it is
                        // filtered out of the chat entirely rather than spamming the transcript.
                        if line.starts_with("ANIM:") { continue; }
                        if proxy.send_event(Ev::Line(line)).is_err() { break; }
                    } else {
                        buf.push(one[0]);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => { engine_gone(&proxy, &resdiag); break; }
            }
        }
    });
}

/// A command from the page: translate (friendly -> wire / hscript) and send it to the
/// engine. Client/Error outcomes are echoed back to the page as SYS lines.
///
/// One chat message may hold several commands separated by blank lines; each is
/// translated and dispatched on its own (in order), so they reach the engine as distinct
/// wire frames with distinct replies. A single (multi-line) command is just the one-block
/// case. See `interpreter::split_commands`.
fn handle_command(text: &str, writer: &SharedWriter, proxy: &EventLoopProxy<Ev>) {
    // SSF2 mode: the SAME Command vocabulary, executed over the reflection bridge.
    // File-IPC blocks (poll loop + per-request waits), so run it off-thread and post
    // replies back through the shared channel router — identical UX to Fraymakers.
    if engine_is_ssf2() {
        let (text, px) = (text.to_string(), proxy.clone());
        thread::spawn(move || {
            for cmd in split_commands(&text) {
                let mut target = crate::ssf2_target::Ssf2Target::new();
                match crate::debug_target::run_command(&mut target, &cmd) {
                    Ok(Some(reply)) => route_ssf2_line(&reply, &px),
                    Ok(None) => {}
                    Err(e) => { let _ = px.send_event(Ev::Line(format!("SYS:ERR:{e}"))); }
                }
            }
        });
        return;
    }
    let send = |w: &str| {
        if w.is_empty() { return; }
        if let Ok(mut g) = writer.lock() {
            if let Some(s) = g.as_mut() {
                let _ = s.write_all(format!("{w}\n").as_bytes());
                let _ = s.flush();
            }
        }
    };
    for cmd in split_commands(text) {
        match translate(&cmd) {
            Translated::Wire(w) => send(&w),
            Translated::Client(t) => { let _ = proxy.send_event(Ev::Line(format!("SYS:OUT:{}", t.trim_end_matches('\n')))); }
        }
    }
}

/// Decode an engine-emitted PNG hex string (`haxe.io.Bytes.toHex`), apply the character's
/// palette-swap map (`<src>><dst> …`, ARGB ints), and return a `data:image/png;base64,…` URL
/// for the matchStatus icon. The base texture is captured un-recolored (the swap is a shader),
/// so we replay the exact-color map here. `None` on malformed/empty hex.
fn icon_data_url(hex: &str, palette: &str) -> Option<String> {
    let bytes = hex_to_bytes(hex)?;
    let png = recolor_png(&bytes, palette).unwrap_or(bytes);
    Some(format!("data:image/png;base64,{}", base64_encode(&png)))
}

/// Hex string -> bytes. `None` on odd length, non-hex chars, or an implausibly short payload.
fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
    let h = hex.trim().as_bytes();
    if h.len() < 16 || !h.len().is_multiple_of(2) { return None; }
    let mut bytes = Vec::with_capacity(h.len() / 2);
    let mut i = 0;
    while i < h.len() {
        let hi = (h[i] as char).to_digit(16)?;
        let lo = (h[i + 1] as char).to_digit(16)?;
        bytes.push(((hi << 4) | lo) as u8);
        i += 2;
    }
    Some(bytes)
}

/// Parse the palette feed (`"<src>><dst> <src>><dst> …"`, signed decimal ARGB ints) into an
/// exact-color replacement map, dropping identity entries (src == dst — colors this palette
/// leaves unchanged). Empty map => nothing to recolor.
fn parse_palette(s: &str) -> std::collections::HashMap<u32, u32> {
    let mut m = std::collections::HashMap::new();
    for tok in s.split_whitespace() {
        if let Some((a, b)) = tok.split_once('>') {
            if let (Ok(src), Ok(dst)) = (a.parse::<i32>(), b.parse::<i32>()) {
                let (s, d) = (src as u32, dst as u32);
                if s != d { m.insert(s, d); }
            }
        }
    }
    m
}

/// Apply the palette map to a PNG: decode, replace each pixel whose ARGB matches a `src` with its
/// `dst`, re-encode. Colors are `0xAARRGGBB` (alpha high byte). `None` (=> caller keeps the base
/// PNG) when there's nothing to do or the image can't be decoded.
fn recolor_png(png: &[u8], palette: &str) -> Option<Vec<u8>> {
    let map = parse_palette(palette);
    if map.is_empty() { return None; }
    let mut rgba = image::load_from_memory(png).ok()?.to_rgba8();
    for px in rgba.pixels_mut() {
        let [r, g, b, a] = px.0;
        let key = ((a as u32) << 24) | ((r as u32) << 16) | ((g as u32) << 8) | (b as u32);
        if let Some(&d) = map.get(&key) {
            px.0 = [((d >> 16) & 0xff) as u8, ((d >> 8) & 0xff) as u8, (d & 0xff) as u8, ((d >> 24) & 0xff) as u8];
        }
    }
    let mut out = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(rgba).write_to(&mut out, image::ImageFormat::Png).ok()?;
    Some(out.into_inner())
}

/// Minimal standard-alphabet base64 (no external dep) — for the icon data: URL.
fn base64_encode(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for ch in data.chunks(3) {
        let b0 = ch[0] as u32;
        let b1 = *ch.get(1).unwrap_or(&0) as u32;
        let b2 = *ch.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        out.push(if ch.len() > 1 { T[((n >> 6) & 63) as usize] as char } else { '=' });
        out.push(if ch.len() > 2 { T[(n & 63) as usize] as char } else { '=' });
    }
    out
}

/// Encode an arbitrary string as a safe JavaScript string literal.
fn js_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod icon_tests {
    use super::*;

    fn png_1px(rgba: [u8; 4]) -> Vec<u8> {
        let mut img = image::RgbaImage::new(1, 1);
        img.put_pixel(0, 0, image::Rgba(rgba));
        let mut buf = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgba8(img).write_to(&mut buf, image::ImageFormat::Png).unwrap();
        buf.into_inner()
    }

    #[test]
    fn parse_palette_skips_identity_and_garbage() {
        // -1 is identity (white->white); -65536 (0xFFFF0000 red) -> -16711936 (0xFF00FF00 green).
        let m = parse_palette("-1>-1 -65536>-16711936 junk 5>x");
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(&0xFFFF0000), Some(&0xFF00FF00));
    }

    #[test]
    fn recolor_swaps_exact_argb_colors() {
        let png = png_1px([0xFF, 0x00, 0x00, 0xFF]); // opaque red
        let out = recolor_png(&png, "-65536>-16711936").unwrap(); // red -> green
        let res = image::load_from_memory(&out).unwrap().to_rgba8();
        assert_eq!(res.get_pixel(0, 0).0, [0x00, 0xFF, 0x00, 0xFF]); // now opaque green
    }

    #[test]
    fn recolor_noop_without_palette_keeps_base() {
        let png = png_1px([0x12, 0x34, 0x56, 0xFF]);
        assert!(recolor_png(&png, "").is_none());          // empty map -> caller keeps base
        assert!(recolor_png(&png, "-1>-1").is_none());     // only identity -> nothing to do
    }

    #[test]
    fn icon_data_url_handles_bad_and_passthrough() {
        assert!(icon_data_url("", "").is_none());          // empty hex
        assert!(icon_data_url("zzzz", "").is_none());      // non-hex
        let hexed: String = png_1px([1, 2, 3, 255]).iter().map(|b| format!("{b:02x}")).collect();
        assert!(icon_data_url(&hexed, "").unwrap().starts_with("data:image/png;base64,"));
    }
}
