require('dotenv').config();
const { MeiliSearch } = require('meilisearch');
const { MongoClient } = require('mongodb');

const MEILISEARCH_HOST = process.env.MEILISEARCH_HOST || 'http://127.0.0.1:7700';
const MEILISEARCH_API_KEY = process.env.MEILISEARCH_API_KEY || '';
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/yourdb';

const client = new MeiliSearch({
  host: MEILISEARCH_HOST,
  apiKey: MEILISEARCH_API_KEY,
});

// ==================== КОНФИГУРАЦИЯ ДЛЯ ТВОЕЙ КОЛЛЕКЦИИ ====================

const COLLECTIONS_TO_SYNC = [
  {
    collection: 'prices', // имя коллекции в MongoDB (строго в нижнем регистре + множественное число обычно)
    index: 'prices',
    primaryKey: 'id',

    // Трансформация документа MongoDB → Meilisearch
    transform: (doc) => ({
      id: doc._id.toString(),
      code: doc.Code || null,
      name: doc.Name || null,
      price: doc.Price || 0,
      balance: doc.Balance || 0,
      measure: doc.Measure || null,

      // Если нужно искать по ID связанных документов — оставляем как строку
      userId: doc.User ? doc.User.toString() : null,
      specOfferId: doc.SpecOffer ? doc.SpecOffer.toString() : null,
      priceId: doc.PriceId ? doc.PriceId.toString() : null,

      // Массивы — приводим к массиву строк (на случай, если там ObjectId)
      category: Array.isArray(doc.Category)
        ? doc.Category.map(c => (typeof c === 'object' ? c.toString() : c))
        : [],
      region: Array.isArray(doc.Region)
        ? doc.Region.map(r => (typeof r === 'object' ? r.toString() : r))
        : [],

      date: doc.Date ? new Date(doc.Date).getTime() : null, // timestamp для сортировки
    }),

    // Настройки поиска — очень важны для фильтров и сортировки!
    settings: {
      searchableAttributes: [
        'name',
        'code',
      ],
      filterableAttributes: [
        'category',
        'region',
        'price',
        'balance',
        'measure',
        'userId',
        'specOfferId',
        'priceId',
      ],
      sortableAttributes: [
        'price',
        'balance',
        'date',
      ],
      rankingRules: [
        'words',
        'typo',
        'proximity',
        'attribute',
        'sort',
        'exactness',
      ],
      typoTolerance: {
        enabled: true,
      },
      pagination: {
        maxTotalHits: 10000,
      },
    },
  },
];

// =====================================================================

async function syncCollection(mongoClient, config) {
  const db = mongoClient.db();
  const collection = db.collection(config.collection);
  const indexName = config.index;

  let index;
  try {
    index = client.index(indexName);
    await index.getRawInfo();
    console.log(`Индекс "${indexName}" уже существует`);
  } catch {
    console.log(`Создаём индекс "${indexName}"...`);
    await client.createIndex(indexName, { primaryKey: 'id' });
    index = client.index(indexName);
  }

  // Применяем настройки
  await index.updateSettings(config.settings);
  console.log(`Настройки индекса "${indexName}" обновлены`);

  // === Полная начальная синхронизация ===
  console.log(`Начальная синхронизация ${config.collection} → ${indexName}...`);
  let total = 0;
  const batchSize = 5000;
  let skip = 0;

  while (true) {
    const docs = await collection.find({}).skip(skip).limit(batchSize).toArray();
    if (docs.length === 0) break;

    const documents = docs
      .map(config.transform)
      .filter(Boolean);

    if (documents.length > 0) {
      await index.addDocuments(documents);
      total += documents.length;
      console.log(`Загружено ${total} документов...`);
    }

    skip += batchSize;
  }

  console.log(`Начальная синхронизация завершена: ${total} документов в "${indexName}"`);

  // === Change Streams (реальное время) ===
  console.log(`Запуск Change Stream для "${config.collection}"...`);
  const pipeline = [
    { $match: { operationType: { $in: ['insert', 'update', 'replace', 'delete'] } } },
  ];

  const changeStream = collection.watch(pipeline, { fullDocument: 'updateLookup' });

  changeStream.on('change', async (change) => {
    try {
      const docId = change.documentKey._id.toString();

      if (change.operationType === 'insert' || change.operationType === 'replace') {
        if (change.fullDocument) {
          const transformed = config.transform(change.fullDocument);
          if (transformed) {
            await index.addDocuments([transformed]);
            console.log(`[+] Добавлен/заменён: ${transformed.name || transformed.code} (${docId})`);
          }
        }
      }

      if (change.operationType === 'update') {
        if (change.fullDocument) {
          const transformed = config.transform(change.fullDocument);
          if (transformed) {
            await index.updateDocuments([transformed]);
            console.log(`[~] Обновлён: ${transformed.name || transformed.code} (${docId})`);
          }
        }
      }

      if (change.operationType === 'delete') {
        await index.deleteDocument(docId);
        console.log(`[-] Удалён документ ${docId}`);
      }
    } catch (err) {
      console.error('Ошибка в Change Stream:', err.message);
    }
  });

  changeStream.on('error', (err) => {
    console.error(`Change Stream ошибка (${config.collection}):`, err.message);
    setTimeout(() => syncCollection(mongoClient, config), 10000);
  });
}

// ==================== ЗАПУСК ====================

async function main() {
  console.log('Подключение к MongoDB...');
  const mongoClient = new MongoClient(MONGO_URI, {
    maxPoolSize: 10,
    serverSelectionTimeoutMS: 5000,
  });

  try {
    await mongoClient.connect();
    console.log('Подключено к MongoDB');

    await client.health();
    console.log('Подключено к Meilisearch');

    for (const config of COLLECTIONS_TO_SYNC) {
      syncCollection(mongoClient, config).catch(console.error);
    }

    process.on('SIGINT', async () => {
      console.log('\nОстановка синхронизатора...');
      await mongoClient.close();
      process.exit(0);
    });

  } catch (err) {
    console.error('Критическая ошибка:', err);
    process.exit(1);
  }
}

main();
