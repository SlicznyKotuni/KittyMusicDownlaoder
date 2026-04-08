"""
Shazam Downloader – automatyczne pobieranie z My Shazam
Python 3.10+
Wymagania: customtkinter, pandas, yt-dlp.exe w tym samym folderze
"""

import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import StringIO
from pathlib import Path
from tkinter import filedialog, messagebox
from typing import Optional

import customtkinter as ctk
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Konfiguracja
# ──────────────────────────────────────────────────────────────────────────────

APP_TITLE    = "Shazam Downloader – automatyczne pobieranie z My Shazam"
APP_SIZE     = "1440x780"
APP_MIN_SIZE = (1300, 700)
MAX_WORKERS  = 4

BASE_DIR           = Path(sys.argv[0]).parent.resolve()
YTDLP_PATH         = BASE_DIR / "yt-dlp.exe"
DOWNLOAD_AUDIO_DIR = BASE_DIR / "Pobrane" / "Audio"
DOWNLOAD_VIDEO_DIR = BASE_DIR / "Pobrane" / "Video"

SEARCH_STRATEGIES = [
    "{artist} - {title} official audio",
    "{artist} - {title} lyrics",
    "{artist} - {title}",
    "{artist} - {title} official video",
]

# Pary kolorów (naprzemiennie parzyste/nieparzyste wiersze) dla każdego panelu
COLORS_BLUE   = ("#16213e", "#0f3460")
COLORS_GREEN  = ("#0d2b0d", "#143d14")
COLORS_RED    = ("#2b0d0d", "#3d1414")
COLORS_ORANGE = ("#2b1a00", "#3d2800")


# ──────────────────────────────────────────────────────────────────────────────
# Funkcje pomocnicze (logika biznesowa)
# ──────────────────────────────────────────────────────────────────────────────

def sanitize_filename(name: str) -> str:
    """Usuwa znaki niedozwolone w nazwach plików."""
    return re.sub(r'[/\\:*?"<>|]', "", name).strip()


def load_shazam_csv(filepath: str) -> pd.DataFrame:
    """
    Wczytuje CSV z My Shazam. Obsługuje nagłówek 'Shazam Library',
    różne separatory i kodowania. Zwraca DataFrame Artist/Title bez duplikatów.
    """
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            content = f.read()
        if content.strip().startswith("Shazam Library"):
            content = content.split("\n", 1)[1]
        df = pd.read_csv(StringIO(content), sep=",", quotechar='"',
                         on_bad_lines="skip", engine="python")
    except Exception as first_err:
        df = None
        for sep in (",", ";", "\t"):
            try:
                df = pd.read_csv(filepath, sep=sep, encoding="utf-8-sig",
                                 quotechar='"', on_bad_lines="skip", engine="python")
                if len(df.columns) >= 4:
                    break
            except Exception:
                continue
        if df is None:
            raise ValueError(f"Nie udało się wczytać CSV.\n{first_err}")

    df.columns = [str(c).strip().lower() for c in df.columns]
    artist_col = next((c for c in ["artist", "wykonawca"] if c in df.columns), None)
    title_col  = next((c for c in ["title", "tytuł", "tytul", "track title", "track"]
                       if c in df.columns), None)

    if not (artist_col and title_col):
        cols = list(df.columns)
        if len(cols) >= 4:
            title_col, artist_col = cols[2], cols[3]
        else:
            raise ValueError(f"Brak kolumn Artist/Title. Kolumny: {list(df.columns)}")

    result = df[[artist_col, title_col]].copy()
    result.columns = ["Artist", "Title"]
    result["Artist"] = result["Artist"].astype(str).str.strip()
    result["Title"]  = result["Title"].astype(str).str.strip()
    result = result[
        result["Artist"].str.strip().astype(bool) &
        result["Title"].str.strip().astype(bool) &
        (result["Artist"].str.lower() != "nan") &
        (result["Title"].str.lower()  != "nan")
    ].copy()
    result["_key"] = (result["Artist"].str.lower() + "|||" + result["Title"].str.lower())
    result = result.drop_duplicates(subset="_key").drop(columns="_key").reset_index(drop=True)
    return result


def check_youtube_availability(
    artist: str, title: str, ytdlp_path: Path, timeout: int = 20
) -> Optional[dict]:
    """
    Wyszukuje utwór na YouTube próbując kolejnych strategii.
    Zwraca dict {url, yt_title, duration} lub None.
    """
    import subprocess
    for strategy in SEARCH_STRATEGIES:
        query = strategy.format(artist=artist, title=title)
        cmd = [str(ytdlp_path), "--flat-playlist",
               "--print", "%(title)s|%(url)s|%(duration)s",
               "--no-warnings", f"ytsearch1:{query}"]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=timeout, encoding="utf-8", errors="replace")
            output = proc.stdout.strip()
            if output:
                parts = output.split("|")
                if len(parts) >= 2:
                    return {"yt_title": parts[0], "url": parts[1],
                            "duration": parts[2] if len(parts) > 2 else "?"}
        except Exception:
            continue
    return None


def download_track(
    artist: str, title: str, url: str, mode: str,
    ytdlp_path: Path, output_dir: Path, timeout: int = 180
) -> tuple[bool, str]:
    """
    Pobiera utwór jako MP3 lub MP4.
    Zwraca (sukces: bool, komunikat_błędu: str).
    Przy sukcesie komunikat jest pusty.
    """
    import subprocess
    safe_name    = sanitize_filename(f"{artist} - {title}")
    out_template = str(output_dir / f"{safe_name}.%(ext)s")

    if mode == "mp3":
        cmd = [str(ytdlp_path), "-x", "--audio-format", "mp3",
               "--audio-quality", "0", "-o", out_template, "--no-warnings", url]
    else:
        cmd = [str(ytdlp_path),
               "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
               "--merge-output-format", "mp4",
               "-o", out_template, "--no-warnings", url]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout, encoding="utf-8", errors="replace")
        if proc.returncode == 0:
            return True, ""
        # Wyciągamy czytelną przyczynę z stderr
        stderr = proc.stderr or ""
        error_lines = [l.strip() for l in stderr.splitlines() if "ERROR" in l.upper()]
        reason = error_lines[0][:120] if error_lines else (stderr.strip()[:120] or "Nieznany błąd yt-dlp")
        return False, reason
    except subprocess.TimeoutExpired:
        return False, f"Timeout ({timeout}s) – zbyt wolne połączenie"
    except Exception as e:
        return False, str(e)[:120]


def save_error_report(failed: list[dict], output_dir: Path) -> Path:
    """Zapisuje raport niepobranych plików do TXT. Zwraca ścieżkę do pliku."""
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"RAPORT_BLEDOW_{ts}.txt"
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("Raport błędów pobierania – Shazam Downloader\n")
        f.write(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Liczba niepobranych: {len(failed)}\n")
        f.write("=" * 70 + "\n\n")
        for i, t in enumerate(failed, 1):
            f.write(f"{i:3}. {t['artist']} – {t['title']}\n")
            f.write(f"     URL  : {t.get('url', 'brak')}\n")
            f.write(f"     Błąd : {t.get('error', 'nieznany')}\n\n")
    return path


# ──────────────────────────────────────────────────────────────────────────────
# Widget: scrollowalny panel listy utworów
# ──────────────────────────────────────────────────────────────────────────────

class TrackListPanel(ctk.CTkFrame):
    """Reużywalny panel z nagłówkiem i scrollowalną listą. Opcjonalna kolumna błędu."""

    def __init__(self, master, title: str, header_color: str = "#1a1a2e",
                 show_error_col: bool = False, **kwargs):
        super().__init__(master, **kwargs)
        self._show_error = show_error_col
        self._rows: list[ctk.CTkFrame] = []
        self._build(title, header_color)

    def _build(self, title: str, header_color: str):
        ctk.CTkLabel(
            self, text=title,
            font=ctk.CTkFont(family="Segoe UI", size=12, weight="bold"),
            fg_color=header_color, corner_radius=6, height=32,
        ).pack(fill="x", padx=4, pady=(4, 2))

        # Nazwy kolumn
        cols_frame = ctk.CTkFrame(self, fg_color="#0d0d1a", height=22)
        cols_frame.pack(fill="x", padx=4, pady=(0, 2))
        cols_frame.pack_propagate(False)
        headers = [("Lp.", 34), ("Wykonawca", 115), ("Tytuł", 115)]
        if self._show_error:
            headers.append(("Przyczyna błędu", 0))  # 0 = rozciągnij resztę
        for i, (text, w) in enumerate(headers):
            lbl = ctk.CTkLabel(
                cols_frame, text=text, width=w if w else 1,
                font=ctk.CTkFont(family="Segoe UI", size=9, weight="bold"),
                text_color="#aaaacc", anchor="w",
            )
            lbl.pack(side="left", padx=(6, 0), fill="x" if w == 0 else None, expand=(w == 0))

        # Obszar scrollowalny
        self.scroll_frame = ctk.CTkScrollableFrame(
            self, fg_color="#0d0d1a", corner_radius=4,
            scrollbar_button_color="#333355",
            scrollbar_button_hover_color="#555577",
        )
        self.scroll_frame.pack(fill="both", expand=True, padx=4, pady=(0, 4))

    def clear(self):
        for row in self._rows:
            row.destroy()
        self._rows.clear()

    def add_track(self, lp: int, artist: str, title: str,
                  color: str = "#1a1a2e", error: str = ""):
        row = ctk.CTkFrame(self.scroll_frame, fg_color=color, corner_radius=3, height=24)
        row.pack(fill="x", padx=2, pady=1)
        row.pack_propagate(False)

        entries = [
            (str(lp),                                          34,  "e"),
            (artist[:22] if len(artist) > 22 else artist,     115, "w"),
            (title[:26]  if len(title)  > 26 else title,      115, "w"),
        ]
        for text, w, anchor in entries:
            ctk.CTkLabel(row, text=text, width=w,
                         font=ctk.CTkFont(family="Segoe UI", size=9),
                         text_color="#d0d0e8", anchor=anchor,
                         ).pack(side="left", padx=(4, 0))

        if self._show_error and error:
            # Błąd wypełnia resztę wiersza
            short_err = error[:70] if len(error) > 70 else error
            ctk.CTkLabel(row, text=short_err,
                         font=ctk.CTkFont(family="Segoe UI", size=9),
                         text_color="#ffaa66", anchor="w",
                         ).pack(side="left", padx=(6, 4), fill="x", expand=True)

        self._rows.append(row)

    def get_count(self) -> int:
        return len(self._rows)

    def populate(self, tracks: list[dict], colors: tuple[str, str] = COLORS_BLUE):
        """Wypełnia panel listą. Każdy dict: artist, title; opcjonalnie error."""
        self.clear()
        for i, t in enumerate(tracks):
            self.add_track(i + 1, t["artist"], t["title"],
                           color=colors[i % 2], error=t.get("error", ""))


# ──────────────────────────────────────────────────────────────────────────────
# Okno dialogowe: raport niepobranych plików
# ──────────────────────────────────────────────────────────────────────────────

class DownloadReportDialog(ctk.CTkToplevel):
    """Modal z listą niepobranych plików + opcją zapisu raportu i ponowienia."""

    def __init__(self, master, failed: list[dict], output_dir: Path, on_retry):
        super().__init__(master)
        self._failed     = failed
        self._output_dir = output_dir
        self._on_retry   = on_retry

        self.title("Raport pobierania – niepobrane pliki")
        self.geometry("900x560")
        self.minsize(720, 400)
        self.grab_set()
        self.focus_force()
        self._build()

    def _build(self):
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        # Nagłówek
        hdr = ctk.CTkFrame(self, fg_color="#3d0000", corner_radius=0, height=52)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.grid_propagate(False)
        hdr.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            hdr,
            text=f"⚠  Nie pobrano {len(self._failed)} pliku/ów  ·  sprawdź przyczyny poniżej",
            font=ctk.CTkFont(family="Segoe UI", size=13, weight="bold"),
            text_color="#ff9999",
        ).grid(row=0, column=0, padx=16, sticky="w")

        # Lista
        lf = ctk.CTkFrame(self, fg_color="#111122", corner_radius=0)
        lf.grid(row=1, column=0, sticky="nsew")
        lf.grid_rowconfigure(1, weight=1)
        lf.grid_columnconfigure(0, weight=1)

        # Nagłówki kolumn listy
        ch = ctk.CTkFrame(lf, fg_color="#0d0d1a", height=26)
        ch.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 2))
        for text, w in [("Lp.", 42), ("Wykonawca", 165), ("Tytuł", 165), ("Przyczyna błędu", 1)]:
            ctk.CTkLabel(
                ch, text=text, width=w if w > 1 else 1,
                font=ctk.CTkFont(family="Segoe UI", size=10, weight="bold"),
                text_color="#aaaacc", anchor="w",
            ).pack(side="left", padx=(6, 0),
                   fill="x" if w == 1 else None, expand=(w == 1))

        scroll = ctk.CTkScrollableFrame(lf, fg_color="#0d0d1a",
                                        scrollbar_button_color="#333355",
                                        scrollbar_button_hover_color="#555577")
        scroll.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))

        for i, t in enumerate(self._failed):
            rf = ctk.CTkFrame(scroll, fg_color=COLORS_ORANGE[i % 2],
                              corner_radius=3, height=26)
            rf.pack(fill="x", padx=2, pady=1)
            rf.pack_propagate(False)
            err = t.get("error", "nieznany błąd")
            a   = t["artist"]
            ti  = t["title"]
            for text, w, anchor, color in [
                (str(i + 1),                          40,  "e", "#d0d0e8"),
                (a[:24]  if len(a)  > 24 else a,      165, "w", "#d0d0e8"),
                (ti[:24] if len(ti) > 24 else ti,     165, "w", "#d0d0e8"),
                (err,                                  1,   "w", "#ffaa66"),
            ]:
                lbl = ctk.CTkLabel(
                    rf, text=text[:80] if w == 1 else text,
                    width=w if w > 1 else 1,
                    font=ctk.CTkFont(family="Segoe UI", size=10),
                    text_color=color, anchor=anchor,
                )
                lbl.pack(side="left", padx=(4, 0),
                         fill="x" if w == 1 else None, expand=(w == 1))

        # Przyciski dolne
        bb = ctk.CTkFrame(self, fg_color="#0a0a18", corner_radius=0, height=62)
        bb.grid(row=2, column=0, sticky="ew")
        bb.grid_propagate(False)

        ctk.CTkButton(
            bb, text="💾  Zapisz raport TXT",
            command=self._save_report,
            fg_color="#444466", hover_color="#666688",
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
            width=190, height=40,
        ).pack(side="left", padx=(16, 8), pady=10)

        ctk.CTkButton(
            bb, text="🔁  Ponów pobieranie nieudanych",
            command=self._retry,
            fg_color="#8a4000", hover_color="#b35500",
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
            width=240, height=40,
        ).pack(side="left", padx=8, pady=10)

        ctk.CTkButton(
            bb, text="Zamknij",
            command=self.destroy,
            fg_color="#333344", hover_color="#555566",
            font=ctk.CTkFont(family="Segoe UI", size=11),
            width=100, height=40,
        ).pack(side="right", padx=16, pady=10)

    def _save_report(self):
        try:
            path = save_error_report(self._failed, self._output_dir)
            messagebox.showinfo("Raport zapisany", f"Raport zapisano:\n{path}", parent=self)
        except Exception as e:
            messagebox.showerror("Błąd zapisu", str(e), parent=self)

    def _retry(self):
        self.destroy()
        self._on_retry(self._failed)


# ──────────────────────────────────────────────────────────────────────────────
# Główna klasa aplikacji
# ──────────────────────────────────────────────────────────────────────────────

class App(ctk.CTk):
    """Główna klasa aplikacji Shazam Downloader."""

    def __init__(self):
        super().__init__()

        self._all_tracks:       list[dict] = []   # Z CSV (unikalne)
        self._found_tracks:     list[dict] = []   # Znalezione na YT (mają url)
        self._not_found_tracks: list[dict] = []   # Nie znaleziono na YT
        self._failed_downloads: list[dict] = []   # Znalezione, ale nie pobrane

        self._searching  = False
        self._downloading = False
        self._download_total = 0
        self._download_done  = 0
        self._current_mode: str = "mp3"
        self._current_output_dir: Optional[Path] = None

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.title(APP_TITLE)
        self.geometry(APP_SIZE)
        self.minsize(*APP_MIN_SIZE)

        self._check_ytdlp()
        self._build_ui()

    def _check_ytdlp(self):
        if not YTDLP_PATH.exists():
            messagebox.showerror(
                "Brak yt-dlp.exe",
                f"Nie znaleziono yt-dlp.exe w:\n{BASE_DIR}\n\n"
                "Pobierz ze: https://github.com/yt-dlp/yt-dlp/releases\n"
                "i umieść obok main.py",
            )

    # ── Budowanie UI ────────────────────────────────────────────────────────

    def _build_ui(self):
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)
        self._build_top_bar()
        self._build_main_area()
        self._build_bottom_bar()

    def _build_top_bar(self):
        bar = ctk.CTkFrame(self, fg_color="#0a0a18", corner_radius=0, height=56)
        bar.grid(row=0, column=0, sticky="ew")
        bar.grid_propagate(False)
        bar.grid_columnconfigure(2, weight=1)

        ctk.CTkLabel(
            bar, text="◈ SHAZAM DOWNLOADER",
            font=ctk.CTkFont(family="Segoe UI", size=14, weight="bold"),
            text_color="#e94560",
        ).grid(row=0, column=0, padx=(16, 8), pady=8)

        ctk.CTkButton(
            bar, text="📂  Wczytaj CSV z My Shazam",
            command=self._on_load_csv,
            fg_color="#1f538d", hover_color="#2a6abf",
            font=ctk.CTkFont(family="Segoe UI", size=12, weight="bold"),
            width=215, height=36,
        ).grid(row=0, column=1, padx=8, pady=8)

        self.lbl_file_status = ctk.CTkLabel(
            bar, text="Nie wybrano pliku CSV",
            font=ctk.CTkFont(family="Segoe UI", size=11),
            text_color="#777799", anchor="w",
        )
        self.lbl_file_status.grid(row=0, column=2, padx=8, sticky="w")

        ytdlp_ok = YTDLP_PATH.exists()
        ctk.CTkLabel(
            bar,
            text=f"{'✓' if ytdlp_ok else '✗'} yt-dlp.exe",
            font=ctk.CTkFont(family="Segoe UI", size=11),
            text_color="#44cc88" if ytdlp_ok else "#ee4444",
        ).grid(row=0, column=3, padx=(0, 16))

    def _build_main_area(self):
        """
        Układ 4 paneli:
          [0] Shazam – wszystkie   [btn]  [1] Znaleziono   [2] Nie znaleziono YT  [3] Błąd pobierania
        """
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.grid(row=1, column=0, sticky="nsew", padx=8, pady=(4, 0))
        main.grid_rowconfigure(0, weight=1)
        # Wagi kolumn: 4 panele + 1 wąska kolumna z przyciskiem
        main.grid_columnconfigure(0, weight=3)   # Wszystkie
        main.grid_columnconfigure(1, weight=0)   # Przycisk
        main.grid_columnconfigure(2, weight=3)   # Znaleziono
        main.grid_columnconfigure(3, weight=3)   # Nie znaleziono YT
        main.grid_columnconfigure(4, weight=4)   # Błąd pobierania (szersza – ma kolumnę błędu)

        # Panel 1: Wszystkie unikalne
        self.panel_all = TrackListPanel(
            main, title="🎵  Shazam – wszystkie unikatowe",
            header_color="#1a1a3e", fg_color="#111122", corner_radius=8,
        )
        self.panel_all.grid(row=0, column=0, sticky="nsew", padx=(0, 3))

        # Środkowy przycisk sprawdzenia
        mid = ctk.CTkFrame(main, fg_color="transparent", width=150)
        mid.grid(row=0, column=1, sticky="ns", padx=3)
        mid.grid_rowconfigure(0, weight=1)
        mid.grid_columnconfigure(0, weight=1)
        self.btn_check = ctk.CTkButton(
            mid, text="▶  Sprawdź\nYouTube",
            command=self._on_check_youtube,
            fg_color="#c0392b", hover_color="#e74c3c",
            font=ctk.CTkFont(family="Segoe UI", size=12, weight="bold"),
            width=142, height=52, state="disabled",
        )
        self.btn_check.grid(row=0, column=0, pady=20)

        # Panel 2: Znalezione na YouTube
        self.panel_found = TrackListPanel(
            main, title="✅  Znaleziono na YouTube",
            header_color="#1a3e1a", fg_color="#111a11", corner_radius=8,
        )
        self.panel_found.grid(row=0, column=2, sticky="nsew", padx=3)

        # Panel 3: Nie znaleziono na YouTube
        self.panel_missing = TrackListPanel(
            main, title="❌  Nie znaleziono na YouTube",
            header_color="#3e1a1a", fg_color="#1a1111", corner_radius=8,
        )
        self.panel_missing.grid(row=0, column=3, sticky="nsew", padx=3)

        # Panel 4: Błąd pobierania (z kolumną przyczyny)
        self.panel_failed = TrackListPanel(
            main, title="⚠  Błąd pobierania",
            header_color="#3e2800", fg_color="#1a1200", corner_radius=8,
            show_error_col=True,
        )
        self.panel_failed.grid(row=0, column=4, sticky="nsew", padx=(3, 0))

    def _build_bottom_bar(self):
        bar = ctk.CTkFrame(self, fg_color="#0a0a18", corner_radius=0, height=88)
        bar.grid(row=2, column=0, sticky="ew")
        bar.grid_propagate(False)
        bar.grid_columnconfigure(5, weight=1)  # Kolumna postępu

        self.btn_mp3 = ctk.CTkButton(
            bar, text="⬇  Pobierz wszystko\njako MP3",
            command=lambda: self._on_download("mp3"),
            fg_color="#1f538d", hover_color="#2a6abf",
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
            width=175, height=52, state="disabled",
        )
        self.btn_mp3.grid(row=0, column=0, padx=(16, 6), pady=16)

        self.btn_mp4 = ctk.CTkButton(
            bar, text="⬇  Pobierz wszystko\njako MP4 (teledysk)",
            command=lambda: self._on_download("mp4"),
            fg_color="#5d3a8a", hover_color="#7b52b3",
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
            width=190, height=52, state="disabled",
        )
        self.btn_mp4.grid(row=0, column=1, padx=6, pady=16)

        # Przycisk ponowienia nieudanych (aktywny tylko gdy są błędy)
        self.btn_retry = ctk.CTkButton(
            bar, text="🔁  Ponów nieudane",
            command=self._on_retry_failed,
            fg_color="#8a4000", hover_color="#b35500",
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
            width=168, height=52, state="disabled",
        )
        self.btn_retry.grid(row=0, column=2, padx=6, pady=16)

        # Przycisk raportu błędów
        self.btn_report = ctk.CTkButton(
            bar, text="📋  Raport błędów",
            command=self._show_report,
            fg_color="#444466", hover_color="#666688",
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
            width=148, height=52, state="disabled",
        )
        self.btn_report.grid(row=0, column=3, padx=(6, 12), pady=16)

        ctk.CTkFrame(bar, fg_color="#333344", width=2, height=58).grid(
            row=0, column=4, sticky="ns", padx=4
        )

        # Postęp
        pf = ctk.CTkFrame(bar, fg_color="transparent")
        pf.grid(row=0, column=5, sticky="ew", padx=(8, 16))
        pf.grid_columnconfigure(0, weight=1)

        self.lbl_progress = ctk.CTkLabel(
            pf, text="Gotowy do pracy",
            font=ctk.CTkFont(family="Segoe UI", size=11),
            text_color="#aaaacc", anchor="w",
        )
        self.lbl_progress.grid(row=0, column=0, sticky="ew")

        self.progress_bar = ctk.CTkProgressBar(
            pf, fg_color="#1a1a2e", progress_color="#e94560",
            height=14, corner_radius=6,
        )
        self.progress_bar.set(0)
        self.progress_bar.grid(row=1, column=0, sticky="ew", pady=(4, 0))

    # ── Zdarzenia GUI ──────────────────────────────────────────────────────

    def _on_load_csv(self):
        """Otwiera dialog wyboru pliku CSV i wczytuje dane."""
        filepath = filedialog.askopenfilename(
            title="Wybierz plik CSV z My Shazam",
            filetypes=[("Pliki CSV", "*.csv"), ("Wszystkie pliki", "*.*")],
        )
        if not filepath:
            return
        try:
            df = load_shazam_csv(filepath)
        except Exception as e:
            messagebox.showerror("Błąd wczytywania", str(e))
            return

        self._all_tracks = [{"artist": r["Artist"], "title": r["Title"]}
                            for _, r in df.iterrows()]
        self._found_tracks.clear()
        self._not_found_tracks.clear()
        self._failed_downloads.clear()

        self.panel_all.populate(self._all_tracks, COLORS_BLUE)
        self.panel_found.clear()
        self.panel_missing.clear()
        self.panel_failed.clear()

        count = len(self._all_tracks)
        self.lbl_file_status.configure(
            text=f"📄 {Path(filepath).name}  |  {count} unikatowych utworów",
            text_color="#88aacc",
        )
        self.lbl_progress.configure(text=f"Wczytano {count} utworów. Kliknij '▶ Sprawdź YouTube'.")
        self.progress_bar.set(0)
        self.btn_retry.configure(state="disabled")
        self.btn_report.configure(state="disabled")

        if YTDLP_PATH.exists() and count > 0:
            self.btn_check.configure(state="normal")
        self._set_dl_buttons(False)

    def _on_check_youtube(self):
        """Uruchamia sprawdzanie YouTube w osobnym wątku."""
        if not self._all_tracks or self._searching:
            return
        self._searching = True
        self._found_tracks.clear()
        self._not_found_tracks.clear()
        self._failed_downloads.clear()
        self.panel_found.clear()
        self.panel_missing.clear()
        self.panel_failed.clear()
        self.btn_check.configure(state="disabled", text="⏳ Sprawdzam…")
        self.btn_retry.configure(state="disabled")
        self.btn_report.configure(state="disabled")
        self._set_dl_buttons(False)
        self.lbl_progress.configure(text="Trwa sprawdzanie dostępności na YouTube…")
        self.progress_bar.set(0)
        threading.Thread(target=self._run_youtube_check, daemon=True).start()

    def _run_youtube_check(self):
        """Wątek: sprawdza każdy utwór na YouTube i aktualizuje listy."""
        total = len(self._all_tracks)
        for i, track in enumerate(self._all_tracks, 1):
            result = check_youtube_availability(track["artist"], track["title"], YTDLP_PATH)
            if result:
                self._found_tracks.append({**track, **result})
            else:
                self._not_found_tracks.append(track)
            self.after(0, self._update_check_ui, i, total)
        self.after(0, self._finish_youtube_check)

    def _update_check_ui(self, done: int, total: int):
        """Aktualizuje UI podczas sprawdzania (wywołanie z głównego wątku)."""
        self.lbl_progress.configure(
            text=f"Sprawdzanie: {done}/{total}  |  "
                 f"✅ Znaleziono: {len(self._found_tracks)}  "
                 f"❌ Nie znaleziono: {len(self._not_found_tracks)}"
        )
        self.progress_bar.set(done / total)
        self.panel_found.populate(self._found_tracks, COLORS_GREEN)
        self.panel_missing.populate(self._not_found_tracks, COLORS_RED)

    def _finish_youtube_check(self):
        """Finalizuje sprawdzanie YouTube."""
        self._searching = False
        f, m = len(self._found_tracks), len(self._not_found_tracks)
        self.btn_check.configure(state="normal", text="▶  Sprawdź\nYouTube")
        self.lbl_progress.configure(
            text=f"Sprawdzanie zakończone.  ✅ Znaleziono: {f}  ❌ Nie znaleziono: {m}"
        )
        self.progress_bar.set(1.0)
        if f > 0 and YTDLP_PATH.exists():
            self._set_dl_buttons(True)

    def _on_download(self, mode: str):
        """Uruchamia pobieranie wszystkich znalezionych."""
        if not self._found_tracks or self._downloading:
            return
        self._start_download(list(self._found_tracks), mode)

    def _on_retry_failed(self, tracks: Optional[list[dict]] = None):
        """Ponawia pobieranie nieudanych plików."""
        retry_list = tracks if tracks is not None else list(self._failed_downloads)
        if not retry_list or self._downloading:
            return
        # Czyścimy panel błędów i listę błędów przed nową próbą
        self._failed_downloads.clear()
        self.panel_failed.clear()
        self._start_download(retry_list, self._current_mode)

    def _start_download(self, tracks: list[dict], mode: str):
        """Przygotowuje i uruchamia pobieranie (nowe lub ponowienie)."""
        output_dir = DOWNLOAD_AUDIO_DIR if mode == "mp3" else DOWNLOAD_VIDEO_DIR
        output_dir.mkdir(parents=True, exist_ok=True)

        self._current_mode       = mode
        self._current_output_dir = output_dir
        self._downloading        = True
        self._download_total     = len(tracks)
        self._download_done      = 0

        self._set_dl_buttons(False)
        self.btn_check.configure(state="disabled")
        self.btn_retry.configure(state="disabled")
        self.btn_report.configure(state="disabled")
        self.progress_bar.set(0)
        self.lbl_progress.configure(
            text=f"Pobieranie 0/{self._download_total}… ({MAX_WORKERS} równolegle)"
        )
        threading.Thread(
            target=self._run_download,
            args=(list(tracks), mode, output_dir),
            daemon=True,
        ).start()

    def _run_download(self, tracks: list[dict], mode: str, output_dir: Path):
        """
        Wątek: pobiera równolegle, na bieżąco aktualizuje panel błędów,
        zbiera nieudane do self._failed_downloads.
        """
        success_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Mapujemy future → oryginalne dane utworu
            futures = {
                executor.submit(
                    download_track,
                    t["artist"], t["title"], t["url"],
                    mode, YTDLP_PATH, output_dir
                ): t
                for t in tracks
            }

            for future in as_completed(futures):
                track = futures[future]
                ok, error_msg = future.result()

                if ok:
                    success_count += 1
                else:
                    # Zachowujemy pełne info + komunikat błędu
                    failed_entry = {**track, "error": error_msg}
                    self._failed_downloads.append(failed_entry)
                    # Live update panelu błędów w GUI
                    self.after(0, self._add_failed_row, failed_entry)

                self._download_done += 1
                self.after(0, self._update_dl_ui, self._download_done, self._download_total)

        self.after(0, self._finish_download, success_count, str(output_dir))

    def _add_failed_row(self, track: dict):
        """Dodaje jeden wiersz do panelu błędów pobierania (na bieżąco)."""
        idx   = self.panel_failed.get_count() + 1
        color = COLORS_ORANGE[idx % 2]
        self.panel_failed.add_track(idx, track["artist"], track["title"],
                                    color=color, error=track.get("error", ""))

    def _update_dl_ui(self, done: int, total: int):
        """Aktualizuje pasek postępu podczas pobierania."""
        failed_so_far = self.panel_failed.get_count()
        ok_so_far     = done - failed_so_far
        self.lbl_progress.configure(
            text=f"Pobieranie {done}/{total}  |  "
                 f"✅ OK: {ok_so_far}  ⚠ Błędy: {failed_so_far}  "
                 f"({MAX_WORKERS} równolegle)"
        )
        self.progress_bar.set(done / total)

    def _finish_download(self, success: int, output_dir: str):
        """Finalizuje pobieranie i pokazuje raport jeśli są błędy."""
        self._downloading = False
        failed = len(self._failed_downloads)

        self.btn_check.configure(state="normal")
        self._set_dl_buttons(True)

        if failed > 0:
            self.btn_retry.configure(state="normal")
            self.btn_report.configure(state="normal")

        self.lbl_progress.configure(
            text=f"✅ Pobrano: {success}/{self._download_total}  "
                 f"⚠ Błędy: {failed}"
                 + (f"  ·  kliknij '📋 Raport błędów'" if failed else "  ·  Wszystko OK!")
        )
        self.progress_bar.set(1.0)

        if failed > 0:
            # Automatycznie otwieramy raport z opóźnieniem (żeby progress bar zdążył się odświeżyć)
            self.after(300, lambda: DownloadReportDialog(
                self, self._failed_downloads,
                Path(output_dir), self._on_retry_failed
            ))
        else:
            messagebox.showinfo(
                "Pobieranie zakończone",
                f"✅ Zakończono!\nPobrano {success} z {self._download_total} plików.\n\n"
                f"Folder:\n{output_dir}",
            )

    def _show_report(self):
        """Otwiera raport błędów pobierania."""
        if not self._failed_downloads:
            messagebox.showinfo("Brak błędów", "Wszystkie pliki zostały pobrane poprawnie.")
            return
        out = self._current_output_dir or DOWNLOAD_AUDIO_DIR
        DownloadReportDialog(self, self._failed_downloads, out, self._on_retry_failed)

    # ── Helpers ────────────────────────────────────────────────────────────

    def _set_dl_buttons(self, enabled: bool):
        """Włącza/wyłącza przyciski pobierania."""
        state = "normal" if enabled else "disabled"
        self.btn_mp3.configure(state=state)
        self.btn_mp4.configure(state=state)


# ──────────────────────────────────────────────────────────────────────────────
# Punkt wejścia
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = App()
    app.mainloop()
