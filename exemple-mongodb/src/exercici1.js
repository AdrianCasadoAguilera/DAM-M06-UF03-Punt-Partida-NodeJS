const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const xml2js = require('xml2js');
const winston = require('winston');
require('dotenv').config();

const XML_PATH = path.resolve(__dirname, '../../data/Posts.xml');

// Analitza l'arxiu XML i el transforma en objecte JS
async function readAndParseXML(filepath) {
  try {
    const content = fs.readFileSync(filepath, 'utf8');
    const xmlParser = new xml2js.Parser({
      explicitArray: false,
      mergeAttrs: true,
    });

    return new Promise((resolve, reject) => {
      xmlParser.parseString(content, (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  } catch (err) {
    console.error('Error accedint al fitxer XML:', err);
    throw err;
  }
}

// Filtra i transforma els posts rellevants
function extractRelevantPosts(xmlObj) {
  const rawPosts = Array.isArray(xmlObj.posts.row) ? xmlObj.posts.row : [xmlObj.posts.row];

  return rawPosts
    .filter(p => parseInt(p.ViewCount) > 20000)
    .map(p => ({
      question: {
        Id: p.Id,
        PostTypeId: p.PostTypeId,
        AcceptedAnswerId: p.AcceptedAnswerId || null,
        CreationDate: new Date(p.CreationDate),
        Score: parseInt(p.Score) || 0,
        ViewCount: parseInt(p.ViewCount) || 0,
        Body: p.Body ? p.Body.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&#xA;/g, '\n') : '',
        OwnerUserId: p.OwnerUserId || null,
        LastActivityDate: new Date(p.LastActivityDate),
        Title: p.Title || '',
        Tags: p.Tags ? p.Tags.split('><').map(t => `<${t.replace(/[<>]/g, '')}>`).join('') : '',
        AnswerCount: parseInt(p.AnswerCount) || 0,
        CommentCount: parseInt(p.CommentCount) || 0,
        FavoriteCount: parseInt(p.FavoriteCount) || 0,
        ContentLicense: p.ContentLicense || ''
      }
    }));
}

// Càrrega de dades a MongoDB
async function seedDatabase() {
  const uri = process.env.MONGODB_URI || 'mongodb://root:example@localhost:27017/';
  const mongo = new MongoClient(uri);

  const logsPath = path.resolve(__dirname, '../../data/logs');
  if (!fs.existsSync(logsPath)) fs.mkdirSync(logsPath, { recursive: true });

  const log = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf(({ timestamp, level, message }) => `${timestamp} [${level.toUpperCase()}]: ${message}`)
    ),
    transports: [
      new winston.transports.Console(),
      new winston.transports.File({ filename: path.join(logsPath, 'exercici1.log') })
    ]
  });

  try {
    log.info('Connectant amb MongoDB...');
    await mongo.connect();
    log.info('Connexió establerta correctament.');

    const db = mongo.db('questions_db');
    const postsCollection = db.collection('questions');

    log.info('Llegint contingut XML...');
    const parsedXML = await readAndParseXML(XML_PATH);

    log.info('Extraient dades dels posts...');
    const filteredPosts = extractRelevantPosts(parsedXML);

    log.info('Esborrant contingut anterior...');
    await postsCollection.deleteMany({});

    log.info('Insertant nous documents...');
    const insertionResult = await postsCollection.insertMany(filteredPosts);
    log.info(`Documents afegits: ${insertionResult.insertedCount}`);
    log.info('Procés completat amb èxit.');
    log.info('--------------------------------------------');
  } catch (err) {
    console.error('S’ha produït un error durant el procés:', err);
  } finally {
    await mongo.close();
    console.log('Desconnectat de MongoDB.');
  }
}

// Llançament de la funció principal
seedDatabase();
