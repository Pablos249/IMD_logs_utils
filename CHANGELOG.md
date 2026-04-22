# Changelog

## 0.1.2 - 2026-04-22

- przeniesiono przyciski `Instrukcja`, `Changelog` i `Pomoc` do prawego gornego rogu glownego okna
- dodano otwieranie instrukcji uzytkownika i changelogu bezposrednio wewnatrz aplikacji
- dolaczono `docs/` oraz `CHANGELOG.md` do buildow `exe` i `portable`
- przyspieszono import logow `CLC`, `CCS` i `Conditioning` przez usuniecie dodatkowego przebiegu po pliku do liczenia linii
- ograniczono liczbe commitow do SQLite przez zapis wsadowy oraz odswiezanie progresu co wieksze partie danych
- wlaczono ustawienia SQLite poprawiajace wydajnosc zapisu: `WAL`, `synchronous=NORMAL`, `temp_store=MEMORY` i zwiekszony cache

## 0.1.1 - 2026-04-22

- zmniejszono rozmiar nowych baz `CLC`, `CCS` i `Conditioning` przez usuniecie duplikowania `raw_line` w tabelach pomiarowych
- dodano migracje istniejacych baz do modelu z `entry_id` wskazujacym na rekord surowego logu
- dodano indeksy `entry_id` dla tabel pomiarowych w parserach `CLC`, `CCS` i `Conditioning`
- dodano bezpieczne kompaktowanie SQLite przez `VACUUM`, z tolerancja na brak wolnego miejsca podczas migracji
- wykonano kompaktowanie istniejacych baz lokalnych, co zmniejszylo rozmiary plikow `ccs_logs.db`, `conditioning_logs.db` i `clc_logs.db`
