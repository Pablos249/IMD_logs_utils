# IMD Log Utils - instrukcja uzytkownika

## 1. Cel aplikacji

IMD Log Utils sluzy do:

- wczytywania logow z wielu zrodel,
- przechowywania ich lokalnie w bazach SQLite,
- filtrowania danych po stacji, pliku, transakcji i identyfikatorach,
- przegladania surowych wpisow,
- budowania prostych oraz bardziej zaawansowanych wykresow czasowych,
- konwersji wybranych logow do CSV.

Aplikacja pracuje w trybie portable. Oznacza to, ze dane i ustawienia sa zapisywane obok programu, w katalogu `portable_data`.

## 2. Co jest zapisywane przez aplikacje

Po uruchomieniu aplikacji tworzony jest katalog:

```text
portable_data\
```

W tym katalogu znajduja sie m.in.:

- `settings.ini` - lista stacji i ostatnio wybrana stacja,
- `can_logs.db` - baza logow IMD / CAN,
- `clc_logs.db` - baza logow CLC,
- `conditioning_logs.db` - baza logow Conditioning,
- `ccs_logs.db` - baza logow CCS,
- `eos_logs.db` - baza logow EOS,
- `startup_profile.log` - log profilowania startu, jezeli zostal wlaczony.

## 3. Pierwsze uruchomienie

Po starcie aplikacji wykonaj:

1. Kliknij `+ Add QP`.
2. Wpisz nazwe stacji lub numer urzadzenia.
3. Zatwierdz przyciskiem `Add`.
4. Upewnij sie, ze nowa stacja jest zaznaczona na gorze okna.

Bez wybranej stacji import logow nie zostanie wykonany.

## 4. Uklad okna glownego

Glowne elementy interfejsu:

- gorny pasek z wyborem stacji, przyciskiem dodawania stacji oraz przyciskiem `Pomoc`,
- zakladki dla roznych typow danych,
- status bar na dole, pokazujacy komunikaty oraz numer wersji,
- menu `Pomoc`, zawierajace informacje o aplikacji, wersji oraz lokalizacji danych.

Najwazniejsze zakladki:

- `IMD Logs` - przegladanie i filtrowanie logow CAN,
- `CLC Logs` - przegladanie logow CLC,
- `Conditioning Logs` - przegladanie logow conditioning,
- `CCS Logs` - przegladanie logow komunikacji CCS,
- `Konwerter` - konwersja logow do CSV,
- `Wczytaj dane` - import mieszanych logow z folderu,
- `Wczytaj dane z EOS` - import i przeglad logow EOS,
- `Wizualizacja (3 wykresy)` - widok zaawansowany z trzema wykresami,
- `Wizualizacja` - szybki widok pojedynczego wykresu.

## 5. Typowy sposob pracy

Najczesciej praca z aplikacja wyglada tak:

1. Wybierasz stacje.
2. Importujesz logi.
3. Sprawdzasz, czy dane pojawily sie w odpowiednich zakladkach.
4. Wchodzisz do zakladki `Wizualizacja` albo `Wizualizacja (3 wykresy)`.
5. Odswiezasz liste serii.
6. Wybierasz serie do wykresu.
7. Rysujesz wykres.
8. Zawężasz zakres czasu, ustawiasz osie Y albo zwiekszasz gestosc punktow.

## 6. Import logow pojedynczo z zakladek

### 6.1 IMD Logs

Zakladka `IMD Logs` sluzy do importu i przegladania logow CAN.

Typowy przebieg:

1. Wejdz do `IMD Logs`.
2. Kliknij `Load CAN Logs`.
3. Wybierz jeden lub wiele plikow.
4. Poczekaj na zakonczenie importu.
5. Skorzystaj z:
   - `Filter by CAN ID`,
   - `Loaded files`,
   - paginacji `Page`, `Previous`, `Next`.

W tej zakladce mozna tez usunac wpisy pochodzace z konkretnego pliku przez `Delete from DB`.

### 6.2 CLC Logs

Zakladka `CLC Logs` pozwala importowac pliki CLC i przegladac dane strona po stronie. Dostepne sa:

- filtr po pliku,
- lista zaladowanych plikow,
- paginacja,
- usuwanie danych z bazy po pliku.

### 6.3 Conditioning Logs

Zakladka `Conditioning Logs` dziala podobnie jak `CLC Logs`.

Najczestsze zastosowania:

- sprawdzenie komunikatow i stanow ukladu conditioning,
- filtrowanie po pliku,
- przygotowanie serii do wizualizacji.

### 6.4 CCS Logs

Zakladka `CCS Logs` sluzy do importu i przegladania logow komunikacji CCS / DIN.

Po imporcie dostajesz:

- tabele wpisow,
- filtrowanie po pliku,
- mozliwosc usuwania wpisow z jednego importu.

### 6.5 EOS

Zakladka `Wczytaj dane z EOS` sluzy do importu logow sesji ladowania EOS.

W tej zakladce szczegolnie wazne sa:

- filtr po `Transaction`,
- filtr po zaladowanym pliku,
- ukrywanie pustych wierszy,
- paginacja,
- usuwanie importu z bazy.

Logi EOS sa szczegolnie istotne w trybie wykresow sesji ladowania, bo na ich podstawie dostepne sa transakcje.

## 7. Import mieszanych logow z folderu

Jezeli masz katalog z roznymi typami logow, najwygodniej skorzystac z zakladki `Wczytaj dane`.

### Krok po kroku

1. Wybierz stacje.
2. Otworz zakladke `Wczytaj dane`.
3. Kliknij `Choose folder`.
4. Wskaz katalog z logami.
5. Zdecyduj, czy wlaczyc `Scan subfolders`.
6. Kliknij `Import folder`.
7. Po zakonczeniu sprawdz podsumowanie i tabele wynikow.

Aplikacja:

- rozpoznaje typ pliku,
- importuje go do odpowiedniej zakladki,
- oznacza pliki jako `imported`, `skipped`, `unknown` albo `error`,
- odswieza zakladki, dla ktorych pojawily sie nowe dane.

To jest najlepszy tryb pracy, gdy dostajesz pelny pakiet logow z jednego przypadku.

## 8. Konwerter logow do CSV

Zakladka `Konwerter` sluzy do przeksztalcania logow do formatu CSV.

Masz dwa tryby:

- `Konwerter logow IMD -> CSV`,
- `Konwerter innego logu -> CSV`.

### Krok po kroku

1. Otworz `Konwerter`.
2. Wybierz odpowiedni rodzaj konwersji.
3. Kliknij `Wybierz plik`.
4. Kliknij `Konwertuj do CSV`.
5. Wskaz lokalizacje pliku wynikowego.
6. Poczekaj na zakonczenie.
7. Sprawdz komunikaty w polu `Wyjscie`.

## 9. Wizualizacja - szybkie tworzenie wykresu

Najprostszy sposob na zrobienie wykresu to zakladka `Wizualizacja`.

### 9.1 Szybki scenariusz

1. Upewnij sie, ze dla wybranej stacji sa juz zaimportowane logi.
2. Otworz zakladke `Wizualizacja`.
3. Kliknij `Refresh series`.
4. Na liscie po lewej wybierz jedna lub kilka serii.
5. Kliknij `Plot selected series`.
6. W razie potrzeby zawez zakres czasu suwakami.

### 9.2 Co oznaczaja najwazniejsze kontrolki

- `Mode`
  - `Full time horizon` - wykres z calego dostepnego czasu,
  - `EOS charging session` - wykres ograniczony do jednej transakcji EOS.
- `Transaction` - lista sesji EOS dla wybranej stacji.
- `Padding [min]` - dodatkowy zapas czasu przed i po sesji.
- `Refresh series` - ponowne zbudowanie listy dostepnych serii.
- `Plot selected series` - narysowanie aktualnie zaznaczonych serii.
- `Clear markers` - usuniecie markerow pomiarowych z wykresu.
- `Snap clicks to sample + logs` - klikniecie przykleja sie do najblizszej probki i otwiera podglad logow w poblizu punktu.
- `Break lines on gaps` - rozdziela linie tam, gdzie sa dluzsze braki w danych.
- `Rectangular signal` - rysuje przebieg schodkowy.
- `More points in view` - dogrywa wiecej punktow dla aktualnie widocznego zakresu czasu.

## 10. Wizualizacja - tryb sesji ladowania EOS

Tryb sesji jest bardzo przydatny, gdy chcesz porownac dane IMD, CLC, Conditioning, CCS i EOS tylko w ramach jednej sesji ladowania.

### Krok po kroku

1. Upewnij sie, ze sa wczytane logi EOS z poprawnymi transakcjami.
2. Otworz `Wizualizacja` albo `Wizualizacja (3 wykresy)`.
3. Ustaw `Mode = EOS charging session`.
4. Wybierz transakcje w polu `Transaction`.
5. Ustaw `Padding [min]`, jezeli chcesz widziec czas przed i po sesji.
6. Zaznacz serie po lewej stronie.
7. Kliknij `Plot selected series`.

Na wykresie pojawia sie pionowe linie wyznaczajace poczatek i koniec sesji.

## 11. Wizualizacja - praca na zakresie czasu

Po narysowaniu wykresu mozesz go zawezac:

- suwaki `Start` i `End` sluza do ustawienia widocznego zakresu czasu,
- myszka na osi czasu pozwala przyblizac i oddalac wykres,
- `More points in view` pobiera wiecej probek tylko dla aktualnego okna czasu.

To jest szczegolnie przydatne, gdy:

- logow jest bardzo duzo,
- chcesz wejsc w szczegol jednego zdarzenia,
- interesuje Cie tylko fragment sesji.

## 12. Wizualizacja - znaczniki i logi wokol punktu

Na wykresie mozna pracowac z markerami:

- lewy przycisk myszy ustawia marker lewy,
- prawy przycisk myszy ustawia marker prawy.

W rezultacie mozesz:

- porownac dwie chwile w czasie,
- zobaczyc delte wartosci,
- przejrzec logi najblizej zaznaczonego punktu.

Jezeli wlaczone jest `Snap clicks to sample + logs`, aplikacja:

- znajduje najblizsza probke do klikniecia,
- otwiera okno z najblizszymi wpisami z wielu zrodel.

To jest bardzo wygodne przy analizie przyczyn problemu.

## 13. Wizualizacja - os Y i kilka wykresow

W zakladce `Wizualizacja (3 wykresy)` dostepne sa trzy niezalezne selektory serii.

Mozesz:

- na pierwszym wykresie pokazac np. prad,
- na drugim napiecie,
- na trzecim temperature.

Sekcja `Y axis range` pozwala:

- wybrac wykres,
- ustawic `Y min`,
- ustawic `Y max`,
- kliknac `Apply Y range`,
- albo wrocic do automatycznej skali przez `Auto Y`.

## 14. Filtrowanie i porzadek w danych

W codziennej pracy warto pilnowac porzadku:

- zawsze wybieraj poprawna stacje przed importem,
- po imporcie sprawdz `Loaded files`,
- przy duplikatach import moze zostac oznaczony jako `skipped`,
- gdy chcesz zaczac od nowa, usun dane z konkretnego pliku przez `Delete from DB`.

## 15. Dobre praktyki pracy

Polecany schemat:

1. Dodaj stacje.
2. Wczytaj komplet logow z jednego przypadku przez `Wczytaj dane`.
3. Sprawdz, czy dane sa widoczne w odpowiednich zakladkach.
4. Przejdz do `Wizualizacja`.
5. Zrob najpierw prosty wykres w `Full time horizon`.
6. Potem przejdz do `EOS charging session`, jezeli chcesz analizowac konkretna sesje.
7. Uzyj markerow i okna logow wokol punktu, gdy potrzebujesz korelacji zdarzen.

## 16. Najczestsze problemy

### Nie moge wczytac pliku

Sprawdz:

- czy wybrana jest stacja,
- czy plik ma poprawny format,
- czy nie byl juz wczesniej zaimportowany i oznaczony jako `skipped`.

### Na wykresie nie ma danych

Sprawdz:

- czy kliknales `Refresh series`,
- czy zaznaczyles serie po lewej stronie,
- czy wybrales poprawna stacje,
- czy w trybie `EOS charging session` wskazales transakcje.

### Nie widze serii, ktorej sie spodziewam

Przyczyny moga byc nastepujace:

- brak danych liczbowych w logu,
- zla stacja,
- niewlasciwy tryb pracy,
- zaimportowany inny zestaw plikow niz oczekiwany.

### Po imporcie jest za duzo danych

Uzyj:

- filtrowania po pliku,
- filtrowania po CAN ID lub transakcji,
- zakresu czasu,
- trybu sesji EOS.

## 17. Informacje o wersji i pomocy

W aplikacji dostepne sa:

- przycisk `Pomoc` na gorze okna,
- menu `Pomoc`,
- okno `O aplikacji`,
- okno z numerem wersji,
- informacja o lokalizacji danych portable.

Numer wersji widoczny jest tez na pasku statusu oraz w tytule okna.

## 18. Jak przygotowac nowa wersje programu

Jezeli pracujesz nad repozytorium i chcesz przygotowac nowa wersje:

```powershell
.\release.ps1
```

Przyklady:

```powershell
.\release.ps1
.\release.ps1 -Bump minor
.\release.ps1 -Bump major
.\release.ps1 -Version 1.0.0
```

Skrypt:

- podnosi wersje,
- sprawdza skladnie najwazniejszych plikow,
- buduje przenosna aplikacje,
- tworzy plik ZIP gotowy do przekazania dalej.

## 19. Szybkie podsumowanie

Jesli masz zapamietac tylko jedna sciezke pracy, to:

1. wybierz stacje,
2. wczytaj logi,
3. odswiez serie,
4. zaznacz serie,
5. narysuj wykres,
6. zawez czas,
7. analizuj markery i logi wokol punktu.
