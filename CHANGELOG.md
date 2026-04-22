# Changelog

## 0.1.1 - 2026-04-22

- zmniejszono rozmiar nowych baz `CLC`, `CCS` i `Conditioning` przez usuniecie duplikowania `raw_line` w tabelach pomiarowych
- dodano migracje istniejacych baz do modelu z `entry_id` wskazujacym na rekord surowego logu
- dodano indeksy `entry_id` dla tabel pomiarowych w parserach `CLC`, `CCS` i `Conditioning`
- dodano bezpieczne kompaktowanie SQLite przez `VACUUM`, z tolerancja na brak wolnego miejsca podczas migracji
- wykonano kompaktowanie istniejacych baz lokalnych, co zmniejszylo rozmiary plikow `ccs_logs.db`, `conditioning_logs.db` i `clc_logs.db`
