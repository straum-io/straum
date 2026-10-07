# Decision log

Every decision the user answered, newest last. The vision-guardian reads
this file on every closeout: an accepted decision is binding, and a rejected
option does not come back without new evidence.

Entry format:

````text
## D-<n>. <question>
Date: YYYY-MM-DD
Choice: A | B, amended: <text> | rejected all | deferred
Rejected options: <id — reason>
Card: docs/decisions/archive/cards.md (D-55 and earlier: cards-to-D-55.md)
````

Compact since D-55 A (2026-10-05): full cards live in `archive/`; read them
only when an entry here is not enough.

---

## D-1. Чем инициализировать позицию лидера `local_sequence` при старте
Date: 2026-09-28
Choice: A — `local_sequence` инициализируется из `Recovery::sequence`,
аллокатор correlation — из `Recovery::correlation`. Пользователь добавил
требование: regression test именно на раздельность пространств, со
значениями, которые намеренно различаются (sequence 100 / correlation 700,
после рестарта 101 / 701), а не просто на восстановление двух чисел.
Rejected options: B — отложенное воспроизведение не нужно, дефект
подтверждён чтением кода; C — численное совпадение пространств в обычном
сценарии не является контрактом.
Card: docs/decisions/archive/cards-to-D-55.md
Выполнено: `wal::restore` возвращает `Restored { sequence, correlation }`;
`crates/runtime/src/lib.rs` берёт `restored.sequence` для `local_sequence`
и `restored.correlation` для аллокатора. Тесты:
`a_restart_continues_each_identity_space_on_its_own` (runtime, спейсы
разведены на 600) и ряд 27 матрицы
`a_restarted_leader_publishes_its_sequence_not_its_correlation` (спейсы
разведены на 692). Ряд 27 проверен в обе стороны: при старой инициализации
даёт `replication_catchups == 2` вместо 0.

## D-2. Форма отказа `TooFarBehind`: остановка задачи или повтор с потолком
Date: 2026-09-28
Choice: A — завершение replication task. Пользователь зафиксировал контракт
как уже определённый реализацией и тестами: `TooFarBehind` → release
retention → replication task exits → WAL can compact. Возврат задачи в
retry loop запрещён: иначе retention снова становится зависимой от
неисправного фолловера.
Rejected options: B — повтор на потолке 5 с: удержание §118 остаётся, и
retention снова зависит от сломанного follower; повтор со сбросом backoff
в 50 мс — то самое бесконечное повторение, которое задача требовала
убрать; отдельная метрика «отказан навсегда» — действие одно и названо в
`error`-строке.
Card: docs/decisions/archive/cards-to-D-55.md

## D-3. Что делать с follower, оказавшимся ниже retention floor
Date: 2026-09-28
Choice: A — оставить конечным состоянием и зафиксировать как операторскую
границу. Автоматическое восстановление не придумывать. Пользователь задал
точный состав того, что должно быть задокументировано: WAL фолловера не
пригоден для продолжения replication history; quorum acknowledgements
лидера не аннулируются; новый follower начинает с новой согласованной WAL
history; snapshot/bootstrap protocol отсутствует; и это не должно быть
замаскировано под обычный reconnect.
Rejected options: B — snapshot-путь: в V5 отсутствует, конкретного
требования нет; C — без записанной процедуры; молчаливая пересборка
follower лидером — нарушает V3.1.
Card: docs/decisions/archive/cards-to-D-55.md
Выполнено: docs/replication.md §130, раздел «Reinitialization, and what it
costs», включая полную операторскую процедуру с перезапуском лидера.

## D-4. Научить рукописные бенчи отличать `cargo test` от `cargo bench`
Date: 2026-10-02
Choice: A — защита в пяти рукописных `main()` (e2e: loadgen, survive,
replicate, connhold; storage: commit), чтобы `cargo test --all-targets` их
не гонял.
Отступление в исполнении, без смены цели: в карточке механизмом была
проверка аргумента `--bench`, с обещанием «документы не меняются,
`cargo bench` работает как раньше». При исполнении выяснилось, что карточка
ошибалась в риске: `loadgen`, `survive` и `connhold` запускаются не только
через `cargo bench`, но и напрямую как бинари из `cargo build --release
--bench`, без `--bench` — `scripts/compare-brokers.sh:62`,
`scripts/crash-survival.sh:67`, `docs/benchmarks.md:168` (`./loadgen`, без
аргументов вообще). Проверка `--bench` сломала бы все три. Поставлена
защита по `cfg!(debug_assertions)`: `cargo test` собирает бенчи в debug,
каждый реальный запуск — release (`cargo bench` наследует release-профиль).
Так выполнено всё, что обещал A, а сценарии не меняются.
Одно обещание A буквально не выполнено: «документы не меняются». Документы
о запуске бенчей действительно не тронуты, но комментарий над командами в
CLAUDE.md §17 переписан — прежний текст утверждал, что рукописные бенчи не
отличают тест от бенча, и после защиты стал неверным.
Проверено: `cargo test -p e2e --all-targets` — 27 с вместо 779 с, `-p
storage` — 8 с, все тесты зелёные; `cargo bench -p e2e --bench connhold` и
прямой запуск release-бинаря проходят защиту (печатают своё «--pid is
required»).
Rejected options: B — ловушка оставалась бы для любого ручного
`--all-targets`; C — L-объём и потеря формата отчёта; `test = false` —
проверено, не работает; `RUN_BENCHMARKS=1` — лишний шаг к каждому измерению.
Card: docs/decisions/archive/cards-to-D-55.md

## D-5. Что Stop-гейт должен делать при повторном Stop
Date: 2026-10-02
Choice: A — при повторном Stop гейт пропускает проверку, только если дерево не
менялось с последнего провала (хеш через `.claude/hooks/diff-hash.sh` в
`.claude/state/gate-failed.sha`, удаляется при успехе); распознавание
`stop_hook_active` — та же регулярка с пробелами, что в require-closeout.sh.
Пользователь добавил: к тестам больше не возвращаться.
Выполнено: scripts/agent/gate.sh. Проверено: метка совпадает с текущим
хешем — выход 0 за 0 с; метка устарела — полный прогон, 0 за 7 с, метка
снята.
Rejected options: B — расширяет пропуск; C — правка после провала остаётся
непроверенной.
Card: docs/decisions/archive/cards-to-D-55.md

## D-6. Какой архитектурный шаг следующий после закрытия C4
Date: 2026-10-02
Choice: A, затем порядок A → B → C → D, заданный пользователем.
Критерий A от пользователя: граница `pending_durable` задаётся в единицах
публикаций/записей, а не выводится из `awaiting_quorum × STRAUM_WAL_QUEUE`;
`awaiting_quorum` остаётся механизмом связи `sequence → correlation`, а не
скрытым бюджетом памяти; обратное давление доходит до издателя без потери
состояния.
Почему D не раньше (слова пользователя): симуляция с инъекцией сбоев
доказывала бы поведение системы с уже известной дырой в ограничении памяти.
B — только после A: иначе бенчмарк меряет систему с неопределённым
контрактом памяти. C — после B: бенчмарк и граница дадут фактическую
нагрузку на узел.
Статус B (2026-10-02): измерено на этой машине (Apple M1, только
направление, CLAUDE.md §11). overhead_admission = 24–33 нс на долговечную
публикацию, без выделений памяти, без конкуренции — один поток берёт и
возвращает разрешения (`runtime/benches/admission.rs`); это около 0.1%
процессорного времени публикации при `fsynced` с одним издателем. Сквозь всю
систему разница v0/v1/v2 внутри шума прогонов при всех трёх уровнях.
Обязательство из «Статуса A» выполнено: v0 против v1, `memory` и `fsynced`,
1 издатель и 10 подписчиков — разница внутри шума. Аномалия
(догоняния при `quorum`) объяснена: два механизма, оба были до A. Отчёт —
docs/baselines/apple-m1-dev.md, «D-6 B». Найдено попутно: прежняя таблица
репликации (`replicate.rs`) мерила постановку в очередь, а не PUBACK, и
отозвана. Открыто: подтверждение на Linux-хосте; конкуренция за семафор при
многих издателях не измерена; значение по умолчанию
`STRAUM_MAX_PENDING_DURABLE` B не выбрал — один издатель с окном 100 до
лимита не доходит (D-13).

Статус A (2026-10-02): сделан, кроме отчёта о производительности. Цикл
брокера — горячий путь: на каждое пробуждение добавились длина очереди,
вычитание и `min`, `select!` вырос до четырёх веток, отчёты лога ушли в
свой канал. Не измерено. По CLAUDE.md §11/§17 это обязательство, а не
выбор, и оно переходит в B: B включает прогон BASE (9dfa3bb) против A,
по очереди, не меньше 5 раундов, для `fsynced` и `memory`, 1 издатель и 10
подписчиков — помимо сравнения `quorum` и `fsynced`.

Rejected options: нет — B, C, D упорядочены, а не отклонены.
Card: docs/decisions/archive/cards-to-D-55.md

## D-7. Кого держит граница подтверждений: весь брокер или только издателей
Date: 2026-10-02
Choice: A. Контракт от пользователя: лимит применяется только к публикациям,
создающим `pending_durable`; QoS 0, подписки, подтверждения и прочие команды
под это давление не попадают; точный лимит `STRAUM_MAX_PENDING_DURABLE`
сохраняется; `pending_durable` остаётся источником фактической глубины;
давление возникает до помещения новой долговечной публикации в WAL, а не
постфактум; PUBACK — только через существующий путь долговечности и
кворума. Не делать очередь на каждую сессию — отделить допуск долговечных
публикаций от общего цикла команд. Цену атомарной операции измерить в B.
Rejected options: B — держит всех; C — ломает порядок команд одной сессии.
Card: docs/decisions/archive/cards-to-D-55.md

## D-8. Что делать, когда кворум уже никогда не соберётся
Date: 2026-10-02
Choice: A. Контракт от пользователя: PUBACK не выдаётся; публикации остаются
в ожидании; брокер не переводится в `Poisoned`; кворум остаётся
`QuorumLost`; один диагностический сигнал сообщает оператору, что
подтверждения остановлены из-за недостижимого кворума, — переход, а не
повтор. Протокол не меняется, издатели не закрываются. Можно до B: путь
диагностический, вне горячего пути.
Порядок пользователя: D-7 A → D-8 A → B. B меряет до\/после для `memory`,
`fsynced`, `quorum`; для `quorum` — пропускную способность и задержку при
достижимом кворуме; недостижимый кворум — вопрос корректности, не
базовой производительности.
Rejected options: B — новая сетевая семантика поверх принятой модели
долговечности; C — молчаливый затор.
Card: docs/decisions/archive/cards-to-D-55.md

## D-9. Что делает соединение, когда лог отказал: сейчас PUBACK уходит без записи
Date: 2026-10-02
Choice: A. Контракт от пользователя: инвариант V3.1 буквальный — «no further
publish will be acknowledged»: `submit()` после отказа WAL не превращается в
успешную публикацию; PUBACK не выдаётся; публикация не передаётся брокеру
как принятая; соединение может ждать до остановки. Тест фиксирует именно
последовательность «отказ WAL → следующая публикация → брокер её не
принимает → PUBACK нет».
Rejected options: B — сетевая семантика закрытия, сознательно не
использованная для QuorumLost; C — нарушение V3.1 остаётся.
Card: docs/decisions/archive/cards-to-D-55.md

## D-10. Остановка при потерянном кворуме ждёт весь grace
Date: 2026-10-02
Choice: A, затем B. Контракт от пользователя: закрытие семафора допуска —
первое действие остановки, до ожидания `STRAUM_SHUTDOWN_GRACE_MS`; новые
публикации больше не проходят допуск; ожидающие издатели получают
завершение ожидания и не висят до конца grace; остальная последовательность
остановки сохраняется. B — документационное закрепление в §16, после A.
Порядок пользователя: D-9 A → D-10 A → D-10 B → бенчмарк B из D-6.
Rejected options: C — незадокументированная пауза.
Card: docs/decisions/archive/cards-to-D-55.md

## D-11. Что делать с `replicate.rs`, который мерил постановку в очередь, а не PUBACK
Date: 2026-10-02
Choice: A — исправить границы замера в `replicate.rs` (PUBLISH→PUBACK по packet id, интервал до последнего PUBACK), перемерить, записать.
Rejected options: B — дороже и теряет режим асинхронной реплики; C — бенч печатает не то, что в колонках.
Card: docs/decisions/archive/cards-to-D-55.md

## D-12. Лишние догоняния из лога при `quorum`
Date: 2026-10-02
Choice: A — отложить. Цена мала, корректность не затронута; механизмы и атрибуция записаны в docs/baselines/apple-m1-dev.md.
Rejected options: B — не сейчас; вернуться, если `replication_catchups` станут основой алертов.
Card: docs/decisions/archive/cards-to-D-55.md

## D-13. Что ещё нужно от B, прежде чем двигаться к C
Date: 2026-10-02
Choice: A — двигаться к C по направлению, полученному на Mac. `STRAUM_MAX_PENDING_DURABLE` остаётся «(initial)» и неизмеренным; конкуренция за семафор и значение по умолчанию — в Linux-прогоне вместе с публичными цифрами.
Rejected options: B, C — не сейчас.
Card: docs/decisions/archive/cards-to-D-55.md

## D-14. Отбрасывания QoS 0 не видны оператору
Date: 2026-10-02
Choice: A — вывести `dropped_messages` в метрики, как `backlog_refused`.
Rejected options: B — невидимые потери QoS 0 при перегрузке.
Card: docs/decisions/archive/cards-to-D-55.md

## D-15. Записывать `dropped_messages` в каждой строке `durability.rs`
Date: 2026-10-02
Choice: A — колонка `dropped_messages` в каждой строке `durability.rs`, снятая с эндпоинта до и после интервала.
Rejected options: B — причина недостачи снова выводилась бы диагностической сборкой.
Card: docs/decisions/archive/cards-to-D-55.md

## D-16. Проверить объяснение 3 702 против 4 270 для 64 KiB
Date: 2026-10-02
Choice: A — временная правка `replicate.rs:58` (2 000 сообщений вместо 400), 5 прогонов, откат.
Rejected options: B — проверка D-11 осталась бы открытой.
Card: docs/decisions/archive/cards-to-D-55.md

## D-17. Гипотеза: асинхронная реплика вдвое режет лидера только из-за общего диска
Date: 2026-10-02
Choice: A — проверить на двух хостах в Linux-прогоне D-13.
Rejected options: B — гипотеза осталась бы открытой.
Card: docs/decisions/archive/cards-to-D-55.md

## D-18. Политика хранения WAL при асинхронной реплике (`written`/`fsynced`)
Date: 2026-10-03
Choice: B — адресуемый хвост не больше R байт при `written`/`fsynced` с фолловером; без фолловера F = голова, как сейчас; триггер «добавлено с прошлой переписи ≥ порога»; R = `STRAUM_WAL_RETAIN_BYTES`, «(initial)» 64 МиБ.
Rejected options: D — те же потери, что B, плюс проводка позиции; A — диск не ограничен; C — изменение формата, риск V4.
Card: docs/decisions/archive/cards-to-D-55.md

## D-19. Сделать §118 верным при перезапуске лидера, независимо от C
Date: 2026-10-03
Choice: A — обход §118 при перезапуске закрывается реализацией D-18 B и D-20 A.
Rejected options: B — не нужна при D-18 B.
Card: docs/decisions/archive/cards-to-D-55.md

## D-20. Политика хранения при `quorum`
Date: 2026-10-03
Choice: A, с отступлением в механизме, объявленным до начала работы: позиция не сохраняется на диске. Отступление подтверждено пользователем 2026-10-03 (D-25: A). При `quorum` после старта `replicated` = 0, и адресуемая область не подрезается, пока фолловер не отчитался; это даёт ту же гарантию — перезапуск лидера не обрывает кворум, — а сохранённая позиция дала бы только раннюю подрезку истории ниже p, экономию диска. Рост лога до первого отчёта ограничен лимитом допуска (D-7).
Rejected options: B — перерыв дольше R стал бы постоянной потерей подтверждений; C — не сейчас.
Card: docs/decisions/archive/cards-to-D-55.md

## D-21. Как остановить лидера, чьё соединение с фолловером принято ядром, но молчит
Date: 2026-10-03
Choice: A
Rejected options: B — тайм-аут рукопожатия; C — отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## D-22. Как не дать догонке отправлять записи, ещё не ставшие надёжными на лидере
Date: 2026-10-03
Choice: A
Rejected options: B — дополнительный пересчёт знака; C — отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## D-23. Пересмотреть D-2: переподключаться ли к отклонённому фолловеру
Date: 2026-10-03
Choice: A
Rejected options: B — оставить D-2; C — отложить до V5.2.
Card: docs/decisions/archive/cards-to-D-55.md

## D-24. Измерить, во что обходится переписывание лога с хвостом R на потоке записи
Date: 2026-10-03
Choice: A
Rejected options: C — сегментированный лог; D — отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## D-25. Подтвердить отступление в D-20: позиция фолловера не сохраняется на диск
Date: 2026-10-03
Choice: A — отступление в D-20 (позиция фолловера не сохраняется на диск) подтверждено пользователем.
Rejected options: B — сохранять позицию.
Card: docs/decisions/archive/cards-to-D-55.md

## D-26. Завершается ли остановка лидера, если фолловер перестал читать посреди догонки или стрима
Date: 2026-10-03
Choice: A
Rejected options: B — ограничить запись только при остановке (не нужен при D-27 A); C — отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## D-27. Нужен ли тайм-аут застоя записи на звене репликации во время работы
Date: 2026-10-03
Choice: A
Rejected options: B — отдельный параметр; C — отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## D-28. Как строить детерминированную симуляцию V5.1 и что именно она доказывает
Date: 2026-10-04
Note (2026-10-04, D-30 C): этап 1 — часы через `Disk::now`, принято.
Choice: A, без дополнения TLA+. Пользователь: «V5.1 в текущей постановке считаю доказательством через детерминированный перебор трасс с инвариантами I1–I10 плюс дедуктивную цепочку C0–C5. Это не формальное доказательство в смысле model checking. Поэтому A + TLA+ не требуется на данном этапе.» Порядок: этап 1 → этап 2 → этап 3 → этап 4.
Rejected options: A + TLA+ (не требуется на этом этапе); B — недетерминизм tokio, изоляция sync_data; C — не покрывает клей; отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## D-31. Сбой синхронизации каталога после переписывания игнорируется, и подтверждённые записи теряются
Date: 2026-10-04
Choice: A
Rejected options: B — повтор fsync (fsyncgate); отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## D-30. Принять ли способ, которым этап 1 подал часы в `storage`
Date: 2026-10-04
Choice: C — часы через Disk::now приняты; строгая модель POSIX для создания файла и синхронизация каталога при создании лога.
Rejected options: A — допущение ext4/xfs; B — длительности снаружи.
Card: docs/decisions/archive/cards-to-D-55.md

## D-29. `written` с фолловером: что происходит с записями, которые фолловер получил, а лидер потерял при отключении питания
Date: 2026-10-04
Choice: A — проверить сценарием S6' на этапе 2 и решать по трассе.
Rejected options: B — документировать до трассы; D — запрет written+follower; отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## D-32. Останавливать ли WAL-писателя сразу, когда переписывание рядом с ним отравило лог
Date: 2026-10-04
Choice: A (ответ пользователя «ok» на рекомендацию A)
Rejected options: B — только тест; отложить.
Card: docs/decisions/archive/cards-to-D-55.md

## 2026-10-04 — Unified Stream adopted

Not a D-card: Stanislav's instruction. `docs/architecture/unified-stream.md`
became the leading architectural source (ADR-0001…0005, commit `9719dcd`).
Open questions from the transition: D-35…D-38. D-33 and D-34 were re-read
against it (notes on the cards).

## D-35. Где в порядке приоритета стоит CONSTITUTION.md
Date: 2026-10-04
Choice: A
Rejected options: B — between CLAUDE.md and ADR; C — merge into CLAUDE.md, constitution superseded whole.
Card: docs/decisions/archive/cards-to-D-55.md

## D-36. Что становится с уровнями надёжности при «commit = большинство, PUBACK и доставка только после commit»
Date: 2026-10-04
Choice: A
Rejected options: B — one semantics everywhere, single node as cluster of one (re-raised as option A of pending D-51, step 8); C — levels everywhere, delivery-after-commit only under quorum.
Card: docs/decisions/archive/cards-to-D-55.md

## D-37. Как оставшиеся этапы D-28 ложатся на фазу 0, и когда проверять `prev_hash`
Date: 2026-10-04
Choice: A
Rejected options: B — prev_hash and slot already in phase-0 v4 (later done by ADR-0008 step 1 under D-48 C); C — finish D-28 stages 3–4 first.
Card: docs/decisions/archive/cards-to-D-55.md

## D-38. Отслеживать ли в git `.claude/`, `docs/decisions/` и `docs/VISION.md`
Date: 2026-10-04
Choice: A
Rejected options: B — mark VISION.md superseded whole, track as archive; C — leave untracked.
Card: docs/decisions/archive/cards-to-D-55.md

## D-39. Закоммитить ли застейдженную работу D-28 (этапы 1–2) и D-30…D-32 отдельным коммитом до фазы 0
Date: 2026-10-04
Choice: A
Rejected options: B — one commit per decision; C — leave to the user.
Card: docs/decisions/archive/cards-to-D-55.md

## D-40. Принять ли, как оформляется первый крейт с unsafe (ADR-0002, п. 3)
Date: 2026-10-04
Choice: A
Rejected options: B — `unsafe_code = "allow"` in the exempt crate; C — decide at the first crate.
Card: docs/decisions/archive/cards-to-D-55.md

## D-34. Принять ли перенос S4 (обрыв на уровне байта) и S9 (застой записи) из этапа 2 в этап 4
Date: 2026-10-04
Choice: A
Rejected options: B — build S4/S9 now in the stage-2 rig: network and time model belong to stage 4; the TCP matrix already covers both.
Card: docs/decisions/archive/cards-to-D-55.md

## D-41. Закоммитить ли правки документов по D-35…D-40 до начала задачи 1 фазы 0
Date: 2026-10-04
Choice: A
Rejected options: B — one docs commit, seed with the first phase-0 code change; C — leave to the user.
Card: docs/decisions/archive/cards-to-D-55.md

## D-42. Что считается «кластерным режимом» (ADR-0006) и что будет с одиночным узлом после фазы 3
Date: 2026-10-04
Choice: A
Rejected options: B — decide now as drafted: belongs to phase-1 quorum design.
Card: docs/decisions/archive/cards-to-D-55.md

## D-43. Журнал после падения процесса считает надёжным то, что было только в ОС
Date: 2026-10-04
Choice: A
Rejected options: B — do not sync at open, report only confirmed: leaves the defect in the model.
Card: docs/decisions/archive/cards-to-D-55.md

## D-48. Как провести пересмотр под новую философию
Date: 2026-10-04
Choice: C — переписать брокер и репликацию сразу под gateway/sequencer.
Rejected options: A — пересмотр артефактов, затем фаза 0 по порядку (рекомендовалось); B — сначала аудит-документ.
Card: docs/decisions/archive/cards-to-D-55.md

Recorded deviation: ADR-0008.

## D-44. Сторона PUBACK в оракуле фазы 0
Date: 2026-10-04
Choice: withdrawn by the agent — taken in the old phase order; superseded by ADR-0008 (D-48 C) and re-read at its step (ADR-0008, Consequences).
Card: docs/decisions/archive/cards-to-D-55.md

## D-45. Копии логики рантайма в harness'е и сужение U1
Date: 2026-10-04
Choice: withdrawn by the agent — taken in the old phase order; superseded by ADR-0008 (D-48 C) and re-read at its step (ADR-0008, Consequences).
Card: docs/decisions/archive/cards-to-D-55.md

## D-46. Как проверить звенья C0 и C5 цепочки D-28
Date: 2026-10-04
Choice: withdrawn by the agent — taken in the old phase order; superseded by ADR-0008 (D-48 C) and re-read at its step (ADR-0008, Consequences).
Card: docs/decisions/archive/cards-to-D-55.md

## D-47. Считается ли потреблённая запись ниже нижней границы «потерянной» (unified-stream §5.1)
Date: 2026-10-04
Choice: withdrawn by the agent — taken in the old phase order; superseded by ADR-0008 (D-48 C) and re-read at its step (ADR-0008, Consequences).
Card: docs/decisions/archive/cards-to-D-55.md

## D-49. Как переходить на новый брокер и репликацию: рядом со старым или на месте
Date: 2026-10-05
Choice: A
Rejected options: B — in place, changing existing crates.
Card: docs/decisions/archive/cards-to-D-55.md

## D-50. Чем считать `prev_hash` батча
Date: 2026-10-05
Choice: A — xxh3-128; unified-stream §4.1 covers Straum's own code, third-party crates are judged as dependencies (CLAUDE.md §2). Recorded in ADR-0002.
Rejected options: B — BLAKE3; C — own 64-bit FNV-1a without dependencies.
Card: docs/decisions/archive/cards-to-D-55.md

## D-33. `written` with a follower silently mixes two histories after a leader power loss
Date: 2026-10-05
Choice: withdrawn (D-55 A) — old path only; the new path has no `written` in a cluster (ADR-0006, ADR-0008). Reproduced by `written_with_a_follower_diverges_when_the_leader_loses_power` in `crates/runtime/tests/simulated_link.rs`.
Card: docs/decisions/archive/cards-to-D-55.md

## D-52. Commit the staged work (`sim` harness, D-43) before the rewrite
Date: 2026-10-05
Choice: withdrawn (D-55 A) — its premise "before the rewrite" has passed; committing is the user's call when they ask.
Card: docs/decisions/archive/cards-to-D-55.md

## D-55. Remove the remaining superseded material
Date: 2026-10-05
Choice: A
Rejected options: B — partial; C — keeps the token cost on every closeout. Deleting old-path code — contradicts D-49 A.
Card: docs/decisions/archive/cards-to-D-55.md

## D-51. What happens to the old path at step 8
Date: 2026-10-05
Choice: A — a single node runs as a cluster of one on the new path (commit = local fsync, delivery after commit); `memory` and `written` go; ADR-0006, D-36 A and the D-42 draft are revised. The two-node `quorum` pair stays on the old path until a witness exists (phase 4); step 8 removes the old path only after that.
Rejected options: B — keep the old path as the single-node mode: two brokers in one product for good.
Card: docs/decisions/archive/cards.md

## D-53. What follows ADR-0008 step 1
Date: 2026-10-05
Choice: A — step 2 as written, in one piece: segmented WAL with refcount + sans-IO sequencer and replica for three nodes + 3-node `sim` (invariants 1, 3, 5, compaction during catch-up).
Rejected options: B — split into 2a/2b; C — Linux baseline first, then B. Wiring v4 into the old path — contradicts D-49 A.
Card: docs/decisions/archive/cards.md

## D-54. Where `prev_hash` lives: per batch or per record
Date: 2026-10-05
Choice: B — every record carries the hash of the previous record, literally as unified-stream §2.2 says.
Rejected options: A — per batch only.
Card: docs/decisions/archive/cards.md

## D-56. Correction to D-54: what unified-stream §2.2 says about `prev_hash`
Date: 2026-10-05
Choice: A — every record carries the hash of the previous batch; the batch hash covers all the batch's record bytes. Both sentences of §2.2 hold; supersedes D-54 B.
Rejected options: B — keep the hash per record as a recorded deviation from §2.2.
Card: docs/decisions/archive/cards.md

## D-57. What the log keeps for a replica that falls behind (re-reads the withdrawn D-47)
Date: 2026-10-05
Choice: A — the sequencer's compaction floor is min(commit, slowest live replica's durable), but the log never keeps more than a byte bound; a replica beyond it is lost and counted. A record compacted after commit counts as held (invariant 1 above the floor).
Rejected options: B — compact to the commit point, lost replicas wait for a state-copy step. Never compacting past a replica — unbounded disk (CLAUDE.md §7).
Card: docs/decisions/archive/cards.md

## D-58. When the sequencer may ship a batch: after its own sync, or in parallel
Date: 2026-10-05
Choice: A — sync, then ship, until step 5; step 5 makes a sequencer restart a takeover with promises and only then lifts the restriction.
Rejected options: B — sync-then-ship permanently. Shipping in parallel now — a sequencer restarting after a power loss could overwrite committed records.
Card: docs/decisions/archive/cards.md

## D-59. The next step
Date: 2026-10-05
Choice: A — step 3 split: 3a a new codec crate (one packet model for 3.1.1 and 5: properties, reason codes, reason strings) and the new contract types; 3b a new gateway crate and the apply function. `mqtt`/`protocol` untouched (D-49 A).
Rejected options: B — step 3 in one piece. Changing `mqtt`/`protocol` in place — contradicts D-49 A. Step 4 before step 3.
Card: docs/decisions/archive/cards.md

## D-60. How a lost replica comes back
Date: 2026-10-05
Choice: A — a state-copy step in ADR-0008 after step 7: a snapshot of the applied state at a committed lsn plus the log after it; the replica resumes with `Chain::resume` there.
Rejected options: B — operator procedure only until phase 4; C — nothing until phase 4.
Card: docs/decisions/archive/cards.md

## D-61. Revise D-59 A: move the contract types from 3a to 3b?
Date: 2026-10-06
Choice: A — D-59 A revised: the protocol-neutral contract types are defined in 3b, with their first user.
Rejected options: B — keep D-59 A and define them before anything uses them (CLAUDE.md §14).
Card: docs/decisions/archive/cards.md

## D-62. The next step
Date: 2026-10-06
Choice: C — split 3b: 3b-1 the apply function and the contract over committed `journal` records, in `sim` against three nodes without MQTT (invariant 4); 3b-2 the gateway over `codec`.
Rejected options: A — 3b in one piece; B — a codec benchmark first (needed before step 4, not before 3b).
Card: docs/decisions/archive/cards.md

## D-63. The next step
Date: 2026-10-06
Choice: A — step 3b-2: the sans-IO gateway over `codec`; session close through the log (`SessionGone`), `apply` bounding its own state from the log (subscriptions, retained in count and bytes); `sim` with MQTT 3.1.1 and 5 clients against three nodes.
Rejected options: B — the codec benchmark first (needed before step 4).
Card: docs/decisions/archive/cards.md

## D-64. Where QoS 1 inflight state lives in 3b-2
Date: 2026-10-06
Choice: A — through the log: an `Ack` record per subscriber PUBACK; `apply` keeps inflight and a bounded backlog per origin, and a full backlog is applied as a slow-consumer close; a gone session's inflight goes with it.
Rejected options: B — local to the gateway until step 7, as a deviation from ADR-0003.
Card: docs/decisions/archive/cards.md

## D-65. Carry MQTT 5 message properties through the log
Date: 2026-10-06
Choice: A — the gateway writes a publish's message properties into the record's properties section; `apply` carries the bytes, opaque, into deliveries and retained messages; the gateway maps them back for MQTT 5 subscribers and drops them for 3.1.1. Message expiry carried, not enforced until step 7.
Rejected options: B — defer to phase 6.
Card: docs/decisions/archive/cards.md

## D-66. The next step
Date: 2026-10-06
Choice: A — step 4 (runtime driver), starting with baselines of `codec`, `apply`, `gateway`, and refusing links between nodes whose `apply` limits differ; D-65 A first.
Rejected options: B — step 5 in simulation first.
Card: docs/decisions/archive/cards.md
