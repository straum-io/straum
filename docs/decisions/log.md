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

## D-67. How the new path keeps its log bounded on disk before state copy exists
Date: 2026-10-07
Choice: B — a local snapshot of the applied state now: `apply::State` encodes and decodes at a committed lsn, the node writes it atomically and compacts below min(compaction floor, snapshot lsn), a restart loads the snapshot and then the log after it. Revises D-60 A for local snapshots; a format version refuses an old snapshot.
Rejected options: A — a hard cap on the log (terminal when full); C — defer to 7b (§7 gap stays).
Card: docs/decisions/archive/cards.md

## D-68. Where step 4's "configuration and its startup checks" go
Date: 2026-10-07
Choice: A — to step 7, where the server stops refusing cluster mode; recorded in ADR-0008.
Rejected options: B — now (configuration nothing reaches, written twice).
Card: docs/decisions/archive/cards.md

## D-69. The next step after D-67 and D-68
Date: 2026-10-07
Choice: A — step 5, ungraceful takeover, on the log as D-67 B leaves it.
Rejected options: B — step-4 leftovers first; C — defer.
Card: docs/decisions/archive/cards.md

## D-70. The next step after step 5
Date: 2026-10-08
Choice: A — harden step 5 in simulation first: the cluster simulation (compaction) and the apply and MQTT simulations run on `cluster::Voter`, with takeovers and compaction during takeover counted; 1000 seeds each. The step-4 test hooks stay for a later card.
Rejected options: B — step 6 first (builds on paths not yet simulated); C — defer. Step 7 before step 6 was ruled out (reverses ADR-0008 order without a reason).
Card: docs/decisions/archive/cards.md

## D-71. When the sequencer ships before its own sync (D-58)
Date: 2026-10-08
Choice: A — defer to unified-stream phase 5 (Linux performance), amending D-58 A: the sequencer keeps syncing before it ships; the parallel ship is a phase-5 candidate, taken only on a Linux measurement of the commit-latency gain.
Rejected options: B — take it now (an unmeasured latency claim, §7).
Card: docs/decisions/archive/cards.md

## D-75. What keeps the old replication path, and how it leaves
Date: 2026-10-08
Choice: A — follow ADR-0008 as planned — plus C's `.gitignore` for `graphify-out/`. Conditions:
1. The old path is frozen: bug fixes only, no new features.
2. Before the old path is removed, the guarantees of the D-28 chain (C1–C4, held today by `sim/tests/chain.rs` reading the old broker and runtime) are carried over into simulation invariants of the new path.
3. Before it is removed, an equivalent durability benchmark exists for the new path, comparable with the current baseline (`e2e/benches/durability.rs`, docs/baselines/).
4. Graphify resolves symbols by name: the docs say "check bridges manually".
Rejected options: B — drop the two-node `quorum` mode now (reverses D-51 A; a supported deployment would disappear before the new path is in the server).
Card: docs/decisions/archive/cards.md

## D-74. The next step
Date: 2026-10-08
Choice: superseded — not answered by option. The D-75 answer said "then proceed to step 6 of ADR-0008", and step 6 went ahead without D-73 A's ordering; rotation is off by default, and 2 000 cluster seeds measured 92 unplaceable replicas with rotation against 89 without. D-73 stays open.
Card: docs/decisions/archive/cards.md

## D-76. Old-path e2e test that fails in the Linux container
Date: 2026-10-09
Choice: A — run it 20 times in the container, tell a test-sizing bug from a real stall-detection bug, fix it as a D-75 bug fix, and name any other container failure.
Rejected options: ignoring it on Linux (removes the only production-platform test of the §8 stall row).
Card: docs/decisions/archive/cards.md

## D-77. The next step after step 6
Date: 2026-10-09
Choice: A — D-73 A first (slot table in the snapshot, version 2), then step 7 as its own card.
Rejected options: none rejected outright; B (step 7 directly), C (D-28 C1–C4 port now) and D (step-6 gaps) stay possible later.
Card: docs/decisions/archive/cards.md

## D-73. Repairing "unplaceable" replicas (the open D-57 A deviation)
Date: 2026-10-09
Choice: A, through D-77 A — the slot table travels in the `apply` snapshot (version 2, version 1 refused), restart merges it with the table rebuilt from the log after it, and the cluster simulation's `unplaceable` exemption goes.
Rejected options: compaction keeping each slot's first segment (the log would never compact under a stable sequencer); keeping the exemption permanently (contradicts D-57 A).
Card: docs/decisions/archive/cards.md

## D-79. The slot table past 4096 slot changes
Date: 2026-10-09
Choice: A — supersedes D-73 A's "in the `apply` snapshot" part: `apply::State` no longer keeps the table (its digest drops the term); the node keeps the committed slot table — built only from committed records — writes it in its snapshot file beside the state, and prunes it by the compaction floor, so no count bound and no remainder of the D-57 A deviation.
Rejected options: none outright; B (raise the count bound, accept the rest) not chosen. No bound at all was rejected on the card (CLAUDE.md §7).
Card: docs/decisions/archive/cards.md

## D-78. Step 7 of ADR-0008
Date: 2026-10-09
Choice: A — after D-79: 7a, the server can start a node on the new path behind an experimental switch that refuses wills, persistent sessions and expiry (nothing handled locally), with config and startup checks, a HELLO frame version and an operator `hand_off`; ADR-0008:125 updated with it. Then 7c, sessions, expiry and will through the log. D-72 is settled before 7a makes the switch reachable.
Rejected options: none outright; B (step 7 whole) and C (D-28 C1–C4 port first) not chosen.
Card: docs/decisions/archive/cards.md

## D-72. D-70 A's takeover-rate criterion
Date: 2026-10-09
Choice: B — a fault mode per seed (one seed in three fault-heavy: sequencer crashes and partitions several times as frequent) in the cluster, apply and MQTT simulations; the summary asserts at least one takeover per hundred steps on the fault-heavy seeds and on average.
Outcome (2026-10-09): B's hypothesis measured false, and the mode reverted; reopened as D-80. In the cluster simulation, 1 000 seeds, fault-heavy seeds reached 0.40, 0.40 and 0.42 takeovers per hundred steps with sequencer faults at 4, 8 and 16 per hundred events (0.36 without). Heartbeats added at 20 or 40 per hundred events lowered it to 0.20 and 0.09: they displace the deliveries and syncs a takeover needs. A takeover takes on the order of a hundred-plus events to complete, so the rate per step is bounded by the protocol's round trips, not by how often faults occur. The cluster simulation keeps printing its rate (0.34 over 1 000 seeds). Reverted mode, to rerun it:
- seeds with `seed % 3 == 2`;
- outside calm periods, at the start of `World::event`, `HEAVY` events in a hundred crash the sequencer (a power loss half the time) or isolate it, 50/50;
- the heartbeat variant adds a `tick()` in `HEAVY_TICKS` events in a hundred, in every phase;
- 1 500 steps per seed, `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim --test cluster -- --nocapture`.
Rejected options: A (accept the measured rate and amend D-70 A) not chosen.
Card: docs/decisions/archive/cards.md

## D-80. The takeover-rate criterion after D-72 B was measured
Date: 2026-10-09
Choice: B — count per fault cycle: in one seed in three, a cycle crashes or cuts off the sequencer and runs events until a new slot commits or a bound is reached; the criterion becomes "a takeover follows at least 9 in 10 sequencer faults within the bound", replacing D-70 A's per-step rate.
Outcome (2026-10-09): built in the cluster simulation and measured; 9 in 10 is not met, so it was not turned on as a gate and the code was removed; reopened as D-81. Over 1 000 seeds, the share of cycles in which a later slot commits within the bound:

| Bound (events) | Other faults running | Faults paused during the cycle |
|---|---|---|
| 600 | 4% (17 of 444) | 50% (291 of 581) |
| 2 000 | 9% | 75% |
| 6 000 | 20% | 86% |

What the failed cycles show:
- With faults running, a majority is often down or cut off.
- With faults paused, every node is up and slots keep advancing while nothing commits. The longest log holds a long uncommitted tail (hundreds of records; two seeds in three run with an effectively unbounded `max_uncommitted`), and candidates fetch it one batch per round trip.
- A tested hypothesis was wrong: a candidate keeping followers silent-free while it catches up left the rate unchanged (53%); reverted.
- The rest of the quiet failures is not yet explained.

Cycle: `seed % 3 == 2`, 1 in 100 non-calm steps, crash (power loss half the time) or isolate the sequencer 50/50, success = the committed log's last record in a later slot; `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim --test cluster -- --nocapture`.
Rejected options: A (floors per sweep, = D-72 A) and C (drop the gate) not chosen.
Card: docs/decisions/archive/cards.md

## D-81. Takeover liveness after a sequencer fault
Date: 2026-10-09
Choice: A — confirm the cycle rates on 10 000 seeds; trace the quiet failed cycles to a cause (the unexplained 14% first); make catch-up fetch up to a configured byte bound per round trip; gate the cycle rate at 9 in 10 within 2 000 events on cycles where a majority stays up, keeping the seeds with a near-unbounded uncommitted tail.
Rejected options: B (gate at the measured level) and C (count floors, no liveness gate) not chosen.
Outcome (2026-10-09), 10 000 cluster seeds: in fault cycles where a majority stays up, a later slot commits within 2 000 events in 93.5% (6 807 of 7 281); 98.4% with an uncommitted bound of 4–16, 87.7% without one (80.4% before reads of several batches). The gate (9 in 10) holds over both groups pooled; the unbounded group alone misses it, a recorded deviation (D-82). Tested and dropped: the candidate keepalive (88.9%) and window 64 (89.2%). Found on the way: `apply` left a retried acknowledgement unanswered, so the gateway retried it for ever (MQTT seed 1189), fixed; the takeover simulation asked for a sequencer at the instant one stepped down after `SlotEnd` (seed 6055), fixed.
Card: docs/decisions/archive/cards.md

## D-82. Failover with no bound on the uncommitted tail
Date: 2026-10-09
Choice: A — `max_uncommitted` gets a default chosen for failover as well as memory (the largest tail measured to keep 9 in 10); the cycle gate is per seed group; seeds left unbounded for coverage are gated at their measured level (for that group alone this amends D-81 A and revives D-81 B).
Rejected options: none outright; B (trace first) and C (accept the pooled gate) not chosen.
Outcome (2026-10-09): three seed groups with fault cycles; candidate defaults at 10 000 seeds: 64 → 94.1%, 96 → 91.1%, 128 → 89.1%, ≥256 = no bound (87.7%). Default 64 — a departure from the decision's wording ("the largest tail measured to keep 9 in 10" is 96, at 91.1%): 64 was taken for margin because 96 failed a default-run sample (28 of 32); put to the user in D-84. Gates per group from 300 cycles: 9 in 10 for 4–16 (98.4%) and default (94.1%), 85% for none (87.1%). Throughput cost unmeasured (D-84). Found on the way: `node` recovery left a torn newer-slot batch's slot on an empty log (seed 4045), fixed.
Card: docs/decisions/archive/cards.md

## D-83. A default for `read_bytes`
Date: 2026-10-09
Choice: A — `STRAUM_READ_BYTES`, default 64 KiB, checked at startup against `max_frame` and the largest batch (refusing what is now silently clamped); carried out with step 7a, where configuration from the environment arrives.
Rejected options: none outright; B (derive from `max_frame`) not chosen.
Card: docs/decisions/archive/cards.md

## D-84. The default for `max_uncommitted`: 64 or 96, and what it costs
Date: 2026-10-09
Choice: A — the production cap is chosen from Linux measurements, not from simulation alone: the cap with the highest throughput that meets the target "at least 9 in 10 failovers commit within 1 second". Measured at 64, 96, 128, 256, 1024 and 4096, both curves recorded in docs/baselines/. Until then 64 stays the simulation's default group and nothing in `node` uses it.
Rejected options: B (take 64), C (96, the literal D-82 A), D (4096) not chosen.
Card: docs/decisions/archive/cards.md

## D-85. What bounds failover time: a replica that falls behind
Date: 2026-10-10
Choice: B as the design direction, not final — bound the lag of a replica outside the committing majority by flow control. Conditions from the user: first define formally what is bounded (unacknowledged records, the lsn gap, or the log available to catch up from); the bound must never block commit for an available majority, and must give backpressure or an explicit move to a state copy rather than cost majority availability; implement only after the lag invariants are written down and it is shown that flow control cannot block majority progress. Until then the choice is not final. Production numbers come from Linux only; the container results are preliminary and choose nothing.
Rejected options: none outright; A (measure first), C, D, E not chosen as the direction (A's host measurement is still needed for the cap, D-84 A).
Card: docs/decisions/archive/cards.md

## D-86. The cap on takeover patience
Date: 2026-10-10
Choice: A — keep 64× and measure. The ~9.6 s worst case (50 ms heartbeat, 3 ticks) is a known risk, not accepted by default; check whether repeated failed takeovers become prolonged unavailability once the network has recovered.
Rejected options: none outright; B and C not chosen without more data.
Card: docs/decisions/archive/cards.md

## D-87. Fault cycles that need a state copy
Date: 2026-10-10
Choice: A — rerun with a realistic retention bound; bringing 7b forward on the strength of these 504 cycles is premature. Before closing: confirm that each such cycle's need for a state copy follows from the retention rule, not from a catch-up error or premature deletion.
Rejected options: B (7b forward) premature; C not chosen.
Card: docs/decisions/archive/cards.md

## D-88. Patience grown during a partition slows recovery after it
Date: 2026-10-10
Choice: A — patience doubles only after a takeover that received a majority's promises; a partition without a majority must not inflate the wait of the next recovery. Order: first of D-88..D-91; with a regression test for a partition.
Rejected options: none outright; B (reset on hearing a majority), C (keep and measure), D (lower cap) not chosen.
Card: docs/decisions/archive/cards.md

## D-89. The replica-lag invariants for D-85 B
Date: 2026-10-10
Choice: A1 then A2 — first fix and justify L1–L4 in ADR-0008, then implement as a separate step with checks in simulation. Conditions from the user, L2 above all: before implementation prove that (1) commit of records already appended is never delayed by a lagging replica; (2) the wait before new appends is bounded by T; (3) after T the lagging replica is detached and no longer holds new appends; (4) detachment changes nothing in quorum, durability, fencing or acknowledgement safety; (5) if the remaining replica cannot form the needed majority, bounded failover without a state copy is not promised; (6) a detached replica's return is safe: it catches up from the available log or goes through the state copy. An available majority (network) is not the same as a majority that can safely continue commit.
Rejected options: B (revise first), C (wait for Linux) not chosen.
Card: docs/decisions/archive/cards.md

## D-90. Cycles still progressing at the cap ("slow")
Date: 2026-10-10
Choice: B — no standing allowance for seed 8956. Raising the limit alone fixes nothing: 8956 shows append speed racing catch-up speed. The long test may stay ignored in ordinary `cargo test`, but: a short regression test checks the mechanism behind the delay; an extended sweep checks seed 8956's actual outcome; the new limit is justified by observed behaviour, not chosen so the test passes.
Rejected options: A (allowance listed by seed) — a known exception must not become an indefinite permission to hang; C not chosen.
Card: docs/decisions/archive/cards.md

## D-91. What retention bound is realistic for the new path
Date: 2026-10-10
Choice: A — 64 MiB as the starting default, compatible with the old path, set in step 7a; not considered a proven optimum.
Rejected options: B (derived) not chosen.
Card: docs/decisions/archive/cards.md

## D-92. The promise round no longer gets a longer wait (a consequence of D-88 A)
Date: 2026-10-10
Choice: A — accept the narrowed contract, keeping explicit the condition that a takeover's promise round fits `takeover_ticks` once in a while. 90.4% against the 90% gate is a small margin: monitor for regressions. Do not change patience merely to raise 90.4%: first find which scenarios make up the remaining 9.6% and which progress guarantees apply to them (mandatory before 7a).
Correction (2026-10-10): the 90.4% belongs to the group without an uncommitted bound (gate 85%, D-82 A); the default-bound group is at 96.3%. The card misread the summary line. The misses are classified in ADR-0008 ("D-92 A — what the misses of the 2 000-event measure are").
Rejected options: B (no effect at three voters), C (no new evidence) not chosen.
Card: docs/decisions/archive/cards.md

## D-93. Flow control is rarely exercised in the simulation
Date: 2026-10-10
Choice: A — a slow-replica scenario in the simulation, in which flow control must reach the `T` boundary regularly; verify not only detachment but recovery, rejoin, catch-up and that majority progress is kept. Trigger frequency alone proves nothing: test scenarios where the mechanism is obliged to trigger. Mandatory before 7a.
Rejected options: B (leave it).
Card: docs/decisions/archive/cards.md

## D-94. A Linux host for step 6
Date: 2026-10-10
Choice: C — defer. A cloud VM, if one is available, may serve preliminary diagnostics only, never the choice of production settings.
Rejected options: A, B not chosen now.
Card: docs/decisions/archive/cards.md

## D-95. A short test of the takeover-during-catch-up race
Date: 2026-10-10
Choice: A — a `Voter`-level test, which must show that a takeover during catch-up uses no stale prefix and does not violate fencing. Mandatory before 7a.
Rejected options: B (a ~210 000-event seed).
Card: docs/decisions/archive/cards.md

## D-96. Step 7a while there is no Linux host
Date: 2026-10-10
Choice: A, with a condition — continue the experimental server connection with `lag_bound: None`; flow control stays disabled; quorum, durability and fencing unchanged; unverified settings are not presented as production-ready. 7a continues only after the mandatory items of D-92, D-93, D-95 and D-97.
Rejected options: B (wait for the host).
Card: docs/decisions/archive/cards.md

## D-97. One sweep that did not finish within an hour
Date: 2026-10-10
Choice: A — reproduce or localise the cause; do not attribute it to contention without evidence. Keep the parameters and trace of the unfinished run; check CPU starvation, locks, queue growth, disk activity and stuck tasks. If it cannot be reproduced, document the uncertainty rather than declare the case harmless. Mandatory before 7a.
Rejected options: B (record as contention).
Card: docs/decisions/archive/cards.md

## D-98. A replica with a slow disk starts takeovers it cannot finish
Date: 2026-10-10
Choice: A — an append from the owner of a newer slot counts as the sequencer being alive while the promise of that slot is made durable. It eliminates false takeovers without weakening durability: the message alone permits no commit. Conditions before closing: (1) reproduce the slow-disk scenario at the standard timing; (2) takeovers back to the level without a slow disk; (3) commit keeps moving while the sequencer is healthy, and no replica commits records without the required durability; (4) fencing and the late-message checks of the previous sequencer preserved; (5) a sequencer that has actually died is still taken over — activity must not block a necessary takeover for ever. "The sequencer shows signs of life" and "the new slot is durably promised" are different states: the first may prevent a false suspicion, never stand in for the second in the commit rules.
Rejected options: B (pre-vote), C (leave it) not chosen.
Card: docs/decisions/archive/cards.md

## D-99. The simulation's base timing takes over every ~300 events with no fault
Date: 2026-10-10
Choice: A — base timings stay in the main sweep; the slow replica is tested separately with steady timings, so a parameter change cannot mask a protocol problem.
Rejected options: B not chosen.
Card: docs/decisions/archive/cards.md

## D-100. A detached replica may never re-attach under sustained load
Date: 2026-10-10
Choice: C — defer: flow control is off by default, and changing catch-up or rejoin rules would add a variable before D-98 is resolved.
Rejected options: A, B, D not chosen now.
Card: docs/decisions/archive/cards.md

## D-101. Does 7a wait for D-98?
Date: 2026-10-10
Choice: A — 7a waits until D-98's check passes at the standard timing; the fault affects operation with and without flow control, and starting the new server path now would complicate diagnosis. Not even experimentally until then. D-101 to be reassessed after D-98.
Rejected options: B, C not chosen.
Card: docs/decisions/archive/cards.md

## D-102. D-101 reassessed: start 7a now?
Date: 2026-10-10
Choice: A — start 7a with flow control off (`lag_bound: None`), as an experimental, not production-ready path. Conditions: (1) flow control stays disabled — the new server path is not verified together with the lag policy; (2) D-98 and D-95 stay mandatory regressions: a sign of life never replaces a durable promise, late appends of an old sequencer never cross the fence; (3) the slow-disk gate runs explicitly in release (`cargo test --release -p sim --test cluster -- --ignored a_slow_disk`); (4) the cluster sweep (10 000), takeover (20 000), apply and MQTT (10 000) are repeated, and any safety regression blocks advancing; (5) no production cap and no bounded-failover claim before Linux measurements. 7a may begin; it is not complete or production-ready until the new server path passes its correctness checks and its performance and failover are measured on Linux. B (a second silence reset during this node's own takeover) is a hypothesis touching the candidate–owner interaction: after 7a's results, with the pending-promise test kept.
Rejected options: B not now; C (wait for Linux) not chosen.
Card: docs/decisions/archive/cards.md

## D-103. How the server reaches the new path (7a)
Date: 2026-10-10
Choice: A — `server` depends on `node`; the new path is not routed through `runtime`. Conditions: the new path stays explicitly experimental and off by default; the dependency table is checked so that the new edge creates no transitive bypass through `runtime`.
Rejected options: B (a separate binary) not chosen; routing through `runtime` rejected on the card (D-75).
Card: docs/decisions/archive/cards.md

## D-104. The new path's configuration surface and defaults (7a)
Date: 2026-10-10
Choice: A — dedicated `STRAUM_CLUSTER_*` variables, not mixed with the old broker's. Invalid configuration is refused at startup, never silently corrected: an odd number of voters, at least three; cluster parameters consistent on all nodes; timing parameters and limits validated before any listener opens. Heartbeat 50 ms and a takeover after three heartbeats stay as experimental defaults, not declared optimal or production-grade. The frame version alone does not make nodes compatible: configuration is part of cluster compatibility.
Rejected options: B (no timing defaults), C (a more conservative default) not chosen.
Card: docs/decisions/archive/cards.md

## D-105. How an operator hands the slot over (7a)
Date: 2026-10-10
Choice: A, with a clarification — SIGUSR1 requests a protocol handoff (`SlotEnd`, the successor schedule) under the rules of fencing, quorum and the committed prefix; it never means "exit", nor a forced change of leader. The node keeps serving until the handoff completes or a failure the protocol provides for. Chosen over HTTP because the metrics listener has no authentication.
Rejected options: B (HTTP on the metrics listener), C (rotation only) not chosen.
Card: docs/decisions/archive/cards.md

## D-106. An automated end-to-end test of the server in cluster mode
Date: 2026-10-11
Choice: A (the user's direction, first in order; no letter given) — automate the three-node scenario with three real server processes. It must check: nodes connecting and a QoS 1 publish; delivery to a subscriber; the handoff by SIGUSR1; publications going on after it; stopping and restarting a node; refusal of an incompatible configuration; and that no publish is acknowledged before its commit condition is met — delivery alone does not prove the durability semantics. 7a is an unfinished experimental stage, not an operable cluster.
Rejected options: B (rumqttc in server tests), C (defer) not chosen.
Card: docs/decisions/archive/cards.md

## D-107. Credentials and TLS on the new path
Date: 2026-10-11
Choice: the user's direction, third in order — define the bounds of acceptable use of the experimental listener and design TLS and credentials. Until both exist the new path is acceptable only in an isolated trusted environment; external reach is limited at startup and by the network environment, never by treating the experimental switch as a security mechanism.
Rejected options: none yet; the design is to come.
Card: docs/decisions/archive/cards.md

## D-108. Node tests that pick ports by binding and releasing them
Date: 2026-10-11
Choice: the user's direction, second in order — diagnose first: one failure and three passing runs do not establish a port race. If the cause is the test helper, the tests use isolated resources and release them reliably; if it is in node startup, it is fixed, never masked by retries.
Rejected options: none.
Card: docs/decisions/archive/cards.md

## D-111. Step 8 now: remove the old broker path
Date: 2026-10-11
Choice: the user's request, confirmed against its consequences — remove the old leader/follower broker path completely now, to end conflicts in decision-making. Removed: the crates `broker`, `protocol`, `mqtt`, `replication`, `runtime` and `e2e`; the old leader/follower simulation (`sim`'s library and `tests/seeds.rs`) and the D-28 chain check (`sim/tests/chain.rs`); the fuzz targets on `mqtt`; the broker half of `server`. Kept: `storage` (its `Wal` is what each segment of the new log is), `transport`, `domain`, `auth` (planned for the new path's credentials, D-110). Overrides D-51 A's and D-75's conditions for step 8 — a witness for the two-node `quorum` pair, D-28 C1–C4 carried into new-path invariants, a durability benchmark comparable with the old path's — none of which holds. Consequences accepted: the server runs only the experimental cluster path (not production-ready, D-102 A); no single-node mode (the new path requires three voters, D-104 A); no TLS or credentials until D-110; the old durability levels (`memory`, `written`, `fsynced`, `quorum`) and the old benchmarks and baselines go with it. Reversible through git: the old crates are in commit ffeda02 and earlier.
Rejected options: "stop deciding for it only" (keep the code, closed for decisions) and "remove after a cluster of one" not chosen.

## D-109. A publish made while the slot changes hands is dropped
Date: 2026-10-11
Choice: B, with conditions — each node retains, in order, the proposals it could not hand to a sequencer, within the existing limits, and re-forwards them once a later slot's sequencer is known. A proposal must not be republished into the log without well-defined deduplication semantics and confirmation of the result: QoS 1 permits redelivery, but that does not authorize violating order, fencing or commit rules. Before the implementation is accepted, tests for two cases: a regular handoff, and an emergency failure of the sequencer before the queue is transferred — each showing that no accepted proposal is lost and that nothing commits after the old owner is fenced.
Rejected options: A (forward the queue at the handoff only), C (close the publisher at once), D (keep and document) not chosen.
Card: docs/decisions/archive/cards.md

## D-110. Credentials and TLS on the new path
Date: 2026-10-11
Choice: A — MQTT client security first (authentication at CONNECT, authorization of PUBLISH and SUBSCRIBE), then mutual TLS between nodes. `STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1` is not a substitute for authentication. Sixth in order; the network restrictions are not lifted.
Rejected options: B (cluster secret on links), C (links left to the network) not chosen.
Card: docs/decisions/archive/cards.md

## D-112. What to rebuild on the new path after step 8
Date: 2026-10-11
Choice: A, after D-109 is closed — metrics before Linux benchmarks: commit, queues, catch-up, handoff, replica state. The old durability levels and benchmarks are not restored mechanically: first determine what guarantees the new architecture gives and which measurements are actually needed. No broker features added now.
Rejected options: B (a cluster of one first), C (neither before 7b/7c) not chosen.
Card: docs/decisions/archive/cards.md

## D-113. Closing D-108: the unstable node start
Date: 2026-10-11
Choice: A — 100 container runs with logs preserved, no automatic retries. If the failure does not recur, D-108 closes as not reproduced, with the diagnostics kept.
Rejected options: B (close now) not chosen.
Card: docs/decisions/archive/cards.md

## D-114. The architecture document after step 8
Date: 2026-10-11
Choice: A, widened — update `unified-stream.md` to the current architecture: experimental status explicit, existing mechanisms separated from planned ones.
Rejected options: B (leave it) not chosen.
Card: docs/decisions/archive/cards.md

## D-115. The losses the outbox still has, inside one slot
Date: 2026-10-11
Choice: A — measure first: outbox overflow, proposals lost on a broken link and dropped proposals are counted in D-112's metrics. No sequencer acknowledgements yet: a forwarding acknowledgement must not be confused with a durable commit. Temporary, not acceptance of silent loss: when the outbox finds an earlier proposal lost, the behaviour is explicit and tested; a later proposal committing must not silently erase the unresolved earlier one.
Rejected options: B (acknowledged forwarding), C (close the publisher at once), D (leave it) not chosen.
Card: docs/decisions/archive/cards.md

## D-116. A failing node test leaves its nodes running
Date: 2026-10-11
Choice: A — the test `Cluster` stops its nodes when dropped, on success and on failure; the original test failure stays visible if cleanup also fails.
Rejected options: B (leave it) not chosen.
Card: docs/decisions/archive/cards.md

## D-117. The outbox is not in any deterministic simulation
Date: 2026-10-11
Choice: B — the outbox moves into `cluster`, driven by the takeover simulation (partitions, loss, a fenced owner still alive). Production and simulation share the same deterministic state machine, I/O as events; no imitation model. Tests establish that proposals survive a partition and a takeover, an old owner cannot commit after fencing, and retries create no duplicate committed record; deterministic, no timing sleeps.
Rejected options: A (into `gateway`), C (a model copy in the simulation), D (keep the deviation) not chosen.
Card: docs/decisions/archive/cards.md

## D-118. A takeover by silence seen in the end-to-end test under load
Date: 2026-10-11
Choice: A — keep the requirement that node 0 sequences there; on failure keep every node's log, to tell a regression from an older takeover issue. The assertion is not relaxed to accept either sequencer.
Rejected options: B (accept either node), C (leave as is) not chosen.
Card: docs/decisions/archive/cards.md

## D-119. Is D-109 closed?
Date: 2026-10-11
Choice: A, narrowly — D-109 is resolved for the reported handoff stall, with its known limitations attached. Closing it does not imply that fencing, proposal loss, memory bounds or all takeover cases are proven. D-117 runs before the outbox's behaviour is used as evidence for D-112. Priorities stated with it: the outbox's bound becomes a real resource bound (byte accounting in the metrics; a bounded-memory policy — explicit backpressure or fail-fast, never silent loss — before any production-readiness claim); the simulation covers the actual outbox logic. Order: D-116 → D-117 → D-118 → D-113 → D-112 (with D-115's loss accounting); D-114 independent.
Rejected options: B (open until D-117), C (open until D-115, D-117 and a byte bound) not chosen.
Card: docs/decisions/archive/cards.md

## D-108. Closed as not reproduced (D-113 A)
Date: 2026-10-12
Outcome: 100 container runs of the node and server tests, plus 100 of the server test at a6986ea, logs kept, no retries: no node failed to start in any run. The only failures (6 of 100, and 5 of 100 at a6986ea) are the end-to-end test's handoff assertion, a takeover that predates D-109 (D-122).

## D-120. Proposals stranded in an open slot, and the resend burst after a slot change
Date: 2026-10-12
Choice: A — cap each node's outbox at `max_queue / voters`, rather than grow the sequencer's queue. Check the rounding and the minimum, so a small `max_queue` never gives zero capacity. A full outbox is explicit — a refusal (backpressure or failure), never a silent loss of a proposal.
Rejected options: B (grow the sequencer's queue), C (reopen D-115 with acknowledgements), D (leave it) not chosen.
Card: docs/decisions/archive/cards.md

## D-121. Storage and transport tests leave their directories behind
Date: 2026-10-12
Choice: A — storage and transport tests release the directories and resources they create, on error too. Diagnostic logs are not deleted with temporary files when a test fails: keeping them is separate, explicit behaviour.
Rejected options: B (leave it) not chosen.
Card: docs/decisions/archive/cards.md

## D-122. A restarted node takes a live sequencer's slot over
Date: 2026-10-12
Choice: A — the link's redial backoff is capped at one heartbeat. It must not weaken fencing or the durable-promise requirements. Tests after the fix: a live sequencer, a restarted node and a delayed connection; and that an old owner cannot commit after it is fenced. Order: D-122 → D-120 → D-121 → D-112; before D-112, the automated handoff test green and checked under load. D-112's metrics, in priority: commit and durability (commit point, proposal-to-commit delay, proposals awaiting confirmation); outbox and memory (size and bytes per node, overflow refusals, resends); replication (each replica's durable position, lag to the commit point, catch-up speed); takeover (attempts, reasons, transfer duration, failures, fencing); MQTT (publishes accepted, PUBACKs, deliveries, queue overflows, closed connections) — cheap counters and histograms first, fit for the Linux benchmark.
Rejected options: B (a restarted node waits before counting silence) not chosen.
Card: docs/decisions/archive/cards.md

## D-123. Keep-alives under outbox backpressure
Date: 2026-10-12
Choice: A — measure first. D-112 measures the duration of withheld reads, the longest spell without a read, and the clients closed for keep-alive; and records each node's outbox memory in bytes — with about 85 MiB per node at worst, a count limit is not yet a memory guarantee. A refusal counter does not replace a guarantee of preservation: the metrics distinguish a proposal not yet accepted by the broker from one accepted and awaiting commit. Criterion for D-112: the load handoff test green, fencing verified, and no unexplained proposal loss; D-112 does not start while the 100 load runs are unfinished, and any failure there becomes a go/no-go card, not an automatic move to metrics. Protocol correctness and its performance are not mixed.
Rejected options: B (pause only proposing reads: needs precise packet separation and a bounded hold; PUBACK never before its commit), C (ignore the silence: changes observability, solves nothing) not chosen.
Card: docs/decisions/archive/cards.md

## D-112. Metrics on the new path — go-ahead and requirements
Date: 2026-10-12
Go-ahead: D-112 starts. The corrected fencing test (`a_link_slower_than_the_takeover_window_fences_the_live_owner`, whose assertion now forbids the fenced node sequencing its old slot, not ever sequencing again) runs separately under load; the 200-run cycle is not repeated, the handoff it verified is unchanged.
Stages, in order: (1) commit and durability — proposal acceptance to commit, durable write delays, proposals awaiting commit: where PUBACK delay arises; (2) outbox — entries and bytes per node, fill, backpressure, resends and refusals; (3) replication — each replica's durable position, lag to the commit point, catch-up rate; (4) takeover — reason, duration and result of a role transfer, fencing; (5) MQTT — publishes, PUBACKs, deliveries, connection closes; and D-123's — read blocking time, longest spell without a read, keep-alive closes.
Requirements: proposal states are never mixed — not yet accepted by the broker; accepted and awaiting commit; committed; refused or lost for a named reason. Measurement does not distort the result — cheap counters and histograms first, no allocation per event and no string formatting on the hot path, monotonic clocks, and no metric feeds a commit or takeover decision.
Done means: tests that the metrics update correctly, the outbox overflow observed, and an observed delay attributable to a stage of the message path; then Linux measurements, with the safety and liveness checks kept as mandatory gates.

## D-124. The Linux run: the cost of measuring first, then the measurements
Date: 2026-10-12
Choice: A — on one Linux host, a build with metrics against one without, under identical load, configuration and startup; at least five alternating rounds, medians; throughput, publish-to-PUBACK latency and failover time. Then full metric collection and diagnostic measurements under the D-123 criterion. A difference within the noise is a result, not proof of zero overhead.
Rejected options: B (switchable measuring first) not chosen; C (no overhead check) rejected in the card.
Card: docs/decisions/archive/cards.md

## D-125. Stage 4 incomplete
Date: 2026-10-12
Choice: B — after the first Linux run. The distinction is kept between handoff (a planned role transfer), takeover (the role taken after failure detection) and fencing (the old owner barred from its old slot): different events with different causes of delay.
Rejected options: A (now), C (leave it) not chosen.
Card: docs/decisions/archive/cards.md

## D-126. A metrics listener over the network, or in-process only
Date: 2026-10-12
Choice: A — keep the HTTP listener as built: off by default, loopback by default, at most four scrapes at once, two-second read and write limits. A diagnostic interface, not a monitoring system; an unsecured network stays a deliberate operator decision. It is kept in the documentation and tests as a separate interface with its own constraints.
Rejected options: B (in-process only) not chosen.
Card: docs/decisions/archive/cards.md

## D-127. A quiet Linux host for D-124
Date: 2026-10-12
Choice: A — bare-metal Linux, first on the machine already available; nothing rented until that hardware is checked. Order: (1) the host's noise — no competing load, CPU availability, no hard limits; control runs of the unchanged code; if the noise is comparable to the expected effect, find steadier conditions; (2) `scripts/overhead.sh`, 071db00 against D-112, alternating, five rounds — more rounds rather than reading small differences of medians when the spread is large; (3) the cost of collection with a scraper polling the listener at a fixed frequency, the same load for both; (4) the D-112 diagnostic measurements and the D-123 criteria — where the delay forms (local write and durability, the quorum, outbox fill, read-blocking spells), handoff and takeover apart and not mixed with fencing. D-124 closes only after an accepted Linux run: comparable conditions, host noise low enough, medians and the spread between rounds given, scraping cost checked, and not presented as a universal benchmark of Straum. A Linux VM is acceptable for diagnostics only; its result stays preliminary. The macOS-against-container difference is not attributed to a macOS component without its own diagnosis. No rewriting of metrics or optimising on noisy numbers.
Rejected options: B (a cloud VM, for accepted numbers), C (the container result only) not chosen.
Card: docs/decisions/archive/cards.md

## D-128. Handoff and takeover in the Linux run's diagnostics
Date: 2026-10-13
Choice: A — client-side scenarios, no server change: a handoff (the role passed to the next node) and a takeover (the owner's process killed), each measured apart — the longest interval without a PUBACK, and lost PUBACKs. A PUBACK missing within the window is not a loss: a delayed PUBACK, a retransmission and a publish never acknowledged are told apart, and a duplicate acknowledgement is not counted as another successful publish.
Rejected options: B (leave it with D-125 B) not chosen.
Card: docs/decisions/archive/cards.md

## D-129. When is a Linux host's noise low enough?
Date: 2026-10-13
Choice: A — a host is accepted only if the two control copies differ by no more than 3 % and each copy's spread across rounds is below 3 %; otherwise the run stops after the noise measurement and nothing is declared accepted. Defined exactly: the difference is |median_A − median_B| / median_A; a copy's spread is (max − min) / median over its rounds. The threshold is also a check that the run can detect the expected effect at all: an overhead is read only against the smallest difference the noise run lets this host detect.
Rejected options: B (each host sets its own floor) not chosen.
Card: docs/decisions/archive/cards.md

## D-130. Failover time and D-129's 3 % gate
Date: 2026-10-15
Choice: A — failover is evaluated as a distribution, not held to a spread below 3 %: the share of failovers within 1 s, p50 and p90 over 50 failovers per round, and a separate diagnosis of the slow mode's causes. Amends D-129 A: the 3 % gate stays on the other overhead quantities, where it applies; failover is not excluded from the comparison — it remains the most important indicator of cluster behaviour, evaluated another way. Two modes (about 300 ms and 1 975 ms) look like real behaviour, not imprecision; more runs will not remove them. 50 events is an initial sample, not a guarantee for the tail: if p90 moves substantially between rounds, more events.
Rejected options: B (gate it with 200+ failovers), C (drop failover from D-124) not chosen.
Card: docs/decisions/archive/cards.md

## D-131. A restarted node cannot catch up under load
Date: 2026-10-15
Choice: A — measure `read_bytes` at 64 KiB, 512 KiB and 8 MiB without changing the read algorithm, and without flow control on at the same time. The heartbeat pacing is a hypothesis: a larger read helps show whether read size limits the rate, not by itself that the heartbeat does. Per size: the replica's position rate, its lag growing or shrinking under load, its recovery rate after the load stops, CPU and disk where available, and the effect on the running nodes' commit latency and PUBACK. Then decide on B (the next read on the replica's reply). Code under D-124's accepted comparison is not changed before that run, unless a problem makes the run itself incorrect.
Contract before the state copy (step 7b): a lagging replica does not accept client operations it cannot safely serve; it continues catching up while that is safe and there is progress; a client never waits indefinitely for a replica's recovery — after a bounded wait it gets an explicit refusal or the connection is closed; no progress for an established period leads to a diagnosable state, not indefinite waiting; reachable over TCP is not the right to serve clients — a correct log and an up-to-date applied state are. No hard timeout on the whole catch-up yet: under continued writes it could turn a temporary overload into a cycle of failures and reconnections; measurements first, and an explicit policy for replicas that cannot keep up under load. The client's wait and the replica's recovery are different mechanisms.
Rejected options: B (pipeline now), C (flow control), D (defer) not chosen.
Card: docs/decisions/archive/cards.md
Correction (2026-10-15): the largest `read_bytes` allowed is 4 MiB — half the 8 MiB frame of 7a — not 8 MiB as the card said (a misreading of `docs/configuration.md`, now reworded); a node started with 8 MiB is refused. The measurement uses 64 KiB, 512 KiB and 4 MiB.

## D-132. The catch-up contract: making "no progress" diagnosable
Date: 2026-10-15
Choice: A — export, with no automatic action, on both sides (the sequencer and the lagging replica): the replica's lag behind the commit point, the time since its position last advanced, the rate at which the lag changes, and whether it is available to clients and catching up — enough to tell a slow catch-up, a lag growing under load, and a stop. No client is disconnected on the age of the last progress: closing connections on a timer adds production behaviour before normal recovery intervals are known; measure first, then choose a threshold.
Rejected options: B (close a stalled replica's clients after a bound), C (do nothing until the Linux run) not chosen.
Card: docs/decisions/archive/cards.md

## D-133. What limits catch-up — and D-131 B
Date: 2026-10-15
Choice: A, then B. (A) the three read sizes (64 KiB, 512 KiB, 4 MiB), three rounds each, keeping PUBACK p50/p99, the lag's rate under load and the recovery rate after it, and adding what the first run lacked: commit latency, CPU and disk. (B) the replication window at 256, 1 024 and 4 096 — a separate diagnostic experiment without PUBACK, not a replacement for the MQTT benchmark. One run per size gives no winner; nothing yet shows a larger read solves it — the replica still recovers in about a minute, longer than the clients' 30 s `commit_timeout`. D-131 B (the next read on the replica's reply) deferred until the heartbeat and window hypotheses are tested, so two factors never change at once.
Rejected options: C (profile on Linux first), D (as a separate choice: subsumed — D-131 B deferred) not chosen.
Card: docs/decisions/archive/cards.md

## D-134. When does failover's p90 call for more failovers?
Date: 2026-10-15
Choice: A — a p90 spread above 10 % between rounds raises the count to 200; with 200, p50, p90 and the share within 1 s are still shown. A trigger for more measurement, not proof of statistical significance.
Rejected options: B (3 %), C (no trigger) not chosen.
Card: docs/decisions/archive/cards.md

## D-135. The replication window limits catch-up
Date: 2026-10-15
Choice: A — explain the mechanism and measure the cost before changing anything; the window stays at 256 until that evidence exists. Measure: (1) the replica's progress against heartbeat ticks — positions per tick, and how often a tick brings none; (2) bytes actually in flight at each heartbeat, not the configured record window — whether the window is used in full; (3) the sequencer's commit latency from raw samples, fine enough to see the tail; (4) comparable rounds at 256, 1 024 and 4 096 — same workload, restart timing and measurement boundaries; (5) the outlier (335 MB written, recovery 0) explained from the data before results are aggregated — retransmission, catch-up replay, repeated writes or a measurement artefact, not assumed. CPU totals alone cannot tell useful work from waiting or scheduling: heartbeat-level samples should. Kept apart: supported — read size showed no clear effect; larger windows went with much faster catch-up. Not established — why the window matters, whether heartbeat pacing constrains the sequencer, whether larger windows keep an acceptable commit tail. Still open — D-124. `straum_catching_up` stays an indicator, not a health verdict. D-135 closes only after the mechanism is investigated and the cost measured, not because 4 096 did best. 2026-10-16, D-136 A: the stage is measured — the sequencer's serial read from the log, 14–25 ms — its cause inside (a segment replay per read) read from the code only; still open (D-139). Closed 2026-10-16 by D-139 A: measured — the kept part of a read is 0.01–0.04 ms of it, the rest the replay of its segment before and after it.
Rejected options: B (a catch-up window), C (4 096 fixed), D (wait for the Linux run) not chosen.
Card: docs/decisions/archive/cards.md

## D-136. What sets the ~40 ms round trip that bounds catch-up
Date: 2026-10-16
Choice: A — instrument each stage of the catch-up round trip on the same batch and position boundaries, telling waiting from work, with a matched control run without a restarted replica; results stay preliminary container results, not an accepted Linux run. Not closed by "window ÷ round trip" alone (D-138): that is a throughput model; if investigation stops without the source, the unknown is recorded as an accepted limitation, not as a mechanism. Result (ADR-0008, D-136 A): no round trip — one read from the log at a time, 14–25 ms each, with no queueing and no wait for the report; the window sets what fits in one read. Why a read costs that (a replay of the segment from its start) is read from the code, not measured: D-135 stays open (D-139).
Rejected options: B (close D-135 now, catch-up window after D-124), C (measure only on the Linux host) not chosen.
Card: docs/decisions/archive/cards.md

## D-137. Does D-124's candidate include the D-132 and D-135 instrumentation?
Date: 2026-10-16
Choice: A — measure the current tree as it is, per-send instrumentation included; record that its individual cost is not isolated. Amends D-131's freeze; D-124's result no longer compares with D-127 step (2)'s metrics-only candidate.
Rejected options: B (per-send part behind a feature), C (revert the per-send part) not chosen.
Card: docs/decisions/archive/cards.md

## D-138. Scope and process, 2026-10-16
Date: 2026-10-16
Choice: accepted from a scope-reduction proposal, with boundaries. The first half of item (7) superseded by D-140 (2026-10-16). (1) D-124 no longer gates feature work: container results may support exploratory work and preliminary decisions; a quiet Linux host stays required for accepted performance claims and for tuning defaults — the accepted Linux comparison is a separate gate for those. (4) QoS 2 and shared subscriptions deferred out of the first usable release; existing QoS 1 commit and acknowledgement semantics are not weakened. (5) Persistent sessions first; wills and session expiry stay explicitly refused until implemented. (7) No CLI or web interface; the Prometheus endpoint (and Grafana) is the initial monitoring path. (2) D-136 is not closed by "window ÷ round trip" (see D-136). (6) D-110 order: TLS client authentication first, mutual TLS between nodes later; a trusted network is a deployment assumption, not an authentication mechanism — replication links stay loopback-only by default and any wider bind still needs the explicit experimental opt-in (D-107). Not cut: deterministic simulation, the committed-log boundary for effects, bounded memory, malformed-input handling, security before network exposure. Process: one guardian round only for genuinely low-risk tasks, never for consensus, durability, protocol behaviour, memory bounds or security. The "lower end of 1–3 months" is a guess, not an estimate. Order: record these; finish D-136; then exploratory feature work without waiting for D-124.
Rejected options: none recorded.
Card: none (the user's decision in conversation)

## D-140. A CLI and an administrator console in the next steps
Date: 2026-10-16
Choice: include the ecosystem brief (`docs/ecosystem.md`) in the next steps of development: `rubtsov-stan/straum-cli` (Go) and `rubtsov-stan/straum-console` (full stack). Supersedes the first half of D-138 (7), "no CLI or web interface"; its second half stands: the Prometheus endpoint (and Grafana) stays the initial monitoring path. Everything else in D-138 stands, in particular security before network exposure: an administrative surface is authenticated before it leaves loopback, and a trusted network is never authentication. The core stays authoritative: no client mutates state except through a core operation, and every such effect goes through the log. Found at adoption: both repositories empty; the core has no administrative API and no machine-readable contract — its outside surfaces are MQTT, the read-only metrics endpoint (D-112) and a local SIGUSR1 handoff. Where the work goes in the order is D-141.
Rejected options: none recorded.
Card: none (the user's decision in conversation)

## D-141. Where the CLI and console go in the order, and what their first slice reads
Date: 2026-10-16
Choice: A — a standalone Go CLI (Cobra) whose initial, read-only interface is the existing Prometheus metrics endpoint; the core first establishes and tests the stable metric contract the CLI relies on; the CLI stays independent of the core's implementation, and its contract is the explicitly stabilised surface, not internal names still changing. `straum-console` stays deferred until option C establishes an admin API contract. Given in the straum-cli session, relayed to this one, and confirmed by the user here (2026-10-16), with its amendment. Done in the core: the stable set in `docs/configuration.md` ("Stable metrics", between `stable-metrics` markers), pinned by `node`'s `the_stable_metrics_are_rendered_as_gauges`. Amended 2026-10-16 (relayed from the straum-cli session): D-141 A governs; `straum-cli`'s `docs/coverage.md` is the CLI roadmap; MQTT publish and subscribe commands are permitted as ordinary client operations — only what the gateway already accepts — without any cluster administration. Authentication, TLS, admin APIs, cluster mutations, Viper, code generation, profiles and an interactive mode stay out of scope until their core capabilities and decisions exist. The CLI's coverage check needs the journal's record kinds, the gateway's reason codes and the server's `STRAUM_*` settings; the user chose (2026-10-16) to stabilise them by the same rule as the metrics rather than let the CLI read internals: declared in `docs/configuration.md` ("Stable names", `stable-settings`, `stable-kinds`, `stable-reasons` blocks) and pinned by `server`'s `the_stable_settings_are_the_ones_read`, `journal`'s `the_stable_kinds_are_the_ones_decoded` and `gateway`'s `the_stable_reason_codes_are_the_ones_sent`; the check reads those blocks, not the source. The journal's reserved kinds (16, 17, 18, 32, 33) are a separate `reserved-kinds` block: numbers held, names and meaning not stable until built (the user's choice, 2026-10-16).
Rejected options: B (CLI and console on the metrics), C (admin API first), D (after 7c and D-110) not chosen now; C remains the console's precondition.
Card: docs/decisions/archive/cards.md

## D-139. The catch-up read: confirm why it costs 14–25 ms, then remove the cost
Date: 2026-10-16
Choice: A — instrument the first segment access of each catch-up read and measure what blocks the replica's log thread; rerun D-136's catch-up mode with paired controls; close D-135 only when the measured evidence explains the 14–25 ms read cost; a separate card for the optimisation, from those measurements. Result (ADR-0008, D-139 A): the kept part of a read takes 0.01–0.04 ms; the rest is the replay of the segment before its start and on to its end, 55–63 MB a read at 3.4–4.7 GB/s; the replica's log thread is held by read-backs for apply doing the same. D-135 closed; the optimisation is D-142.
Rejected options: B (a read cursor), C (a sparse index), D (accept as a limitation) not chosen before the measurement; they return in D-142 with its numbers.
Card: docs/decisions/archive/cards.md

## D-142. Stop replaying whole segments for catch-up reads and read-backs
Date: 2026-10-16
Choice: A — (1) stop segment scans as soon as a read or read-back has what it needs; (2) a position-only read cursor per replica and one for apply, so sequential reads resume from the previous position; (3) cursors invalidated on truncation and compaction; (4) each change measured on its own against D-139's baseline before they are combined; (5) the always-on per-read scan counting removed after measurement; (6) the full guardian review, with unified-stream §3's segment-refcount requirement resolved explicitly before the change is accepted. Justified by the measured 55–63 MB of unneeded scanning a read, catch-up commit p99 of 83–131 ms, and replica recovery that can outlast the 30 s `commit_timeout`. Cold-disk performance stays unmeasured, an open risk.
Amended 2026-10-16 after the guardian review: unified-stream §3 is amended (the user's choice over a recorded deviation or a pin for every reader) — a reader that can be in progress during a truncation or compaction holds a pin; reads serialised with them on the log thread need none. And the measurement is rerun to CLAUDE.md §11 — five alternating rounds, windows 256, 1 024 and 4 096, and cap's failover mode — before the change is accepted.
Rejected options: B (a sparse index), C (early stop only), D (accept as a limitation) not chosen.
Card: docs/decisions/archive/cards.md

## D-143. After D-142: what next
Date: 2026-10-16
Choice: A + B; C deferred. A — the main next step: 7c (persistent sessions), starting with a design card, not code. The card defines a global limit on persisted sessions and explicit behaviour at the limit — refusal, never unbounded memory or disk — and how the system recovers from an admission failure, a restart and a partial write error. Implementation of 7c does not begin until these conditions are agreed. Changing a reserved record kind is a separate decision, and the straum-cli session is told first. B — in parallel: catch-up measured on a cold page cache if the container can control it; it does not delay the design. C — the D-124 Linux run, after A and B, unless it blocks the next stage. D-142's limits (cold disk, real sync, Linux, pins for future readers on other threads) stay open risks, not mixed with the new work.
Rejected options: none; C deferred.
Card: docs/decisions/archive/cards.md
