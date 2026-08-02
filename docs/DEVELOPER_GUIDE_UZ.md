# amo2gsheet — yangi developer uchun qisqa qo‘llanma

Bu hujjat norasmiy handoff yozuvi. Asosiy maqsad — bot qayerdan ishga tushishi,
AMO va Google Sheet o‘rtasida ma’lumot qanday yurishi va qaysi sozlamaga tegish
kerakligini tez tushuntirish.

## 1. Bot nima qiladi?

Bot ikki yo‘nalishda ishlaydi:

1. **AMO → Google Sheet**
   - AMO status o‘zgarganda /webhook/amocrm webhook keladi.
   - Bot webhookga darhol 200 qaytaradi, keyin alohida worker ichida ishlaydi.
   - Lead to‘liq qayta olinadi: contact, company va custom fieldlar bilan.
   - Yangi/order trigger statusiga kirgan lead Sheet1 ga yangi qator bo‘lib yoziladi.
   - Oldin kuzatilgan leadning statusi o‘zgarsa, mavjud qatorning Статус katagi yangilanadi.

2. **Google Sheet → AMO**
   - Worker Sheet1 ni SYNC_POLL_SECONDS oralig‘ida o‘qiydi.
   - Заказ № yozilsa, AMO dagi order-number custom field yangilanadi va lead keyingi statusga o‘tadi.
   - Статус dropdown qiymati o‘zgarsa, shu qiymatga mos AMO statusiga PATCH yuboriladi.
   - Успешно Sheet uchun ko‘rsatish qiymati; u AMO ga qayta yuborilmaydi.

Qo‘shimcha ravishda worker webhook yetib kelmay qolgan leadlarni taxminan har 10 daqiqada
AMO dan catch-up qilib tekshiradi.

## 2. Asosiy ishga tushish oqimi

deploy/start.sh quyidagini ishga tushiradi:

    uvicorn sync_service:app --host 0.0.0.0 --port 8000

sync_service.py startup vaqtida:

1. oy almashgan bo‘lsa, Sheet1 ni MM.YYYY nomiga archive qiladi;
2. kerak bo‘lsa yangi Sheet1 yaratadi va canonical header yozadi;
3. .sync_state.json dan leadlar holatini tiklaydi;
4. Sheet → AMO worker va AMO webhook worker threadlarini ishga tushiradi.

Ishchi sikl:

    check_and_rotate_sheet
      → expire_finished_leads
      → sync_sheet_to_amo
      → vaqti-vaqti bilan catch_up_trigger_leads

## 3. Google Sheet tuzilishi

Lead tabining canonical ustunlari sync_service.py ichidagi COLUMNS ro‘yxatida.
Yangi tablar headerni doim shu ro‘yxatdan oladi. Ustun qo‘shilsa, avval shu ro‘yxatga
qo‘shing; mavjud archive tablarni qayta yozmang.

Hozirgi tartib:

    Компания | ID | Заказ № | Ф.И.О. | Контактный номер | Дата заказа |
    Дата доставка | Код сотрудника | Ответственный | Группа | Продукт 1 |
    Количество 1 | Продукт 2 | Количество 2 | Бюджет сделки | Регион |
    Адрес | Тип продажи | Продажа в рассрочку | Воронка | Статус

- Sheet1 — joriy oy uchun active tab.
- O‘tgan oy tablari MM.YYYY ko‘rinishida archive bo‘ladi.
- Staff — № | Код сотрудника | Сотрудник | Отдел; Код сотрудника orqali
  Ответственный aniqlanadi.
- Active tab headeri buzilsa, eski tab Sheet1_CORRUPT_... sifatida saqlanadi
  va yangi canonical Sheet1 yaratiladi.

## 4. AMO field va status ID lari qayerda?

### Status/pipeline ID

- Asosiy manba — AMO API: /api/v4/leads/pipelines?with=statuses.
- Bot startupda pipeline va status ID larni dinamik yuklaydi.
- PIPELINE_ID=0 bo‘lsa, har leadning o‘zidagi joriy pipeline ishlatiladi.
- TRIGGER_STATUS_ID=0 bo‘lsa, trigger status nomi orqali avtomatik topiladi.
- Fallback va qo‘lda mappinglar .env dagi PIPELINE_ID, TRIGGER_STATUS_ID
  va DROPDOWN_STATUS_MAP_JSON orqali beriladi.
- Status nomlarini Sheet qiymatlariga o‘girish STATUS_DISPLAY_MAP,
  SHEET_STATUS_TO_AMO_DISPLAY va AMO_STATUS_TO_SHEET_OVERRIDE orqali bo‘ladi.

### Lead custom fieldlar

Ko‘pchilik ustunlar field ID bilan emas, AMO qaytargan field_name bilan topiladi.
Shuning uchun AMO dagi field nomini o‘zgartirish qatorni bo‘sh qoldirishi mumkin.
Tekshirish uchun:

    GET /leads/custom_fields
    GET /leads/{lead_id}

build_row() quyidagilarni maxsus oladi:

- ID — lead standard ID;
- Бюджет сделки — lead price;
- Компания — embedded company nomi;
- Ф.И.О. va Контактный номер — embedded contactlardan;
- Воронка va Статус — pipeline/status mappingdan;
- qolgan mos ustunlar — custom_fields_values[].field_name orqali.

**Muhim hard-coded AMO field:** order number hozir field_id=987889 sifatida
sync_service.py ichida ishlatiladi. AMO da bu field o‘zgarsa, faqat .env ni
o‘zgartirish yetmaydi — koddagi barcha 987889 joylarini tekshirish kerak.

## 5. Muhim konfiguratsiya fayllari

- .env — production/dev credential, AMO subdomain, Sheet ID, trigger status va polling.
  Bu faylni commit qilmang.
- .env.example — yangi muhit uchun xavfsiz shablon.
- env_loader.py — ENVIRONMENT=dev/prod ga qarab DEV_* yoki PROD_* qiymatlarni tanlaydi.
- prod_gsheet.json — Google service-account key; maxfiy.
- .amo_tokens_prod.json — AMO OAuth tokenlari; maxfiy.
- .sync_state.json — lead qaysi tab/qaysi status/order bilan kuzatilayotganini saqlaydi.
  Bot ishlayotgan paytda qo‘lda o‘chirmang.
- data/ — runtime SQLite va boshqa local data; Google Sheet o‘rnini bosmaydi.

Productionda eng muhim qiymatlar:

    ENVIRONMENT=prod
    PROD_AMO_SUBDOMAIN=...
    PROD_GOOGLE_SHEET_ID=...
    PROD_GOOGLE_SERVICE_ACCOUNT_FILE=prod_gsheet.json
    PROD_GOOGLE_WORKSHEET_NAME=Sheet1
    PROD_TRIGGER_STATUS_NAME=...
    SYNC_POLL_SECONDS=10
    SHEET_ROTATION_INTERVAL=monthly
    DISPLAY_TZ_OFFSET=5

## 6. Core fayllar

- sync_service.py — asosiy FastAPI app, AMO client, Sheet client, webhook va workerlar.
- env_loader.py — environment tanlash.
- setup_sheet.py — bir martalik Sheet header/dropdown/Staff setup.
- inspect_amo.py — AMO pipeline, status va custom fieldlarni ko‘rish.
- prod_check.py — deploydan oldingi aloqa/config audit.
- import_xlsx.py — bir martalik Excel → AMO import; oddiy sync oqimining qismi emas.
- deploy/start.sh, deploy/amo2gsheet.service — production start.
- tests/test_sheet_safety.py — Sheet header/rotation/recovery regression testlar.

## 7. Status bo‘yicha asosiy qoida

- Trigger status (TRIGGER_STATUS_NAME) → yangi qator, odatda Sheet statusi В процессе.
- Заказ № to‘ldirilishi → AMO order field + status update.
- Sheet У курера → AMO dagi muvaffaqiyatli/won statusga o‘tish.
- Sheet Отказ → pipeline reject statusiga o‘tish.
- Sheet Успешно → faqat display; AMO ga PATCH qilinmaydi.
- Webhook duplicate eventlari WEBHOOK_DEDUP_TTL_SEC ichida tashlab yuboriladi.

## 8. Tekshirish va loglar

Asosiy loglar LOG_DIR ichida:

- app.log — umumiy log;
- webhooks.log — webhook kelishi va filtering;
- leads.log — lead yozilishi/status/order holati;
- amo_api.log — AMO request/response statuslari.

Minimal tekshiruv:

    GET /health
    GET /structure
    GET /leads/custom_fields
    GET /leads/{lead_id}

Individual lead diagnostikasi uchun alohida script yozmasdan, logs/ ichidan
lead=<id> bo‘yicha qidiring.

## 9. Qisqa qoida

1. Header o‘zgarishi faqat COLUMNS orqali qilinadi.
2. AMO field nomi o‘zgarsa, GET /leads/custom_fields bilan qayta tekshiriladi.
3. Status nomi o‘zgarsa, mapping va trigger konfiguratsiyasi birga tekshiriladi.
4. .env, token va Google key commit qilinmaydi.
5. Sheet corruptionda eski tab ma’lumotini o‘chirmang; archive/quarantine qiling.
6. Production restartdan oldin barcha importlar mavjudligini, keyin py_compile va
   regression testlarni tekshiring.
