const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const PDFDocument = require('pdfkit');
require('dotenv').config();

const MONGO_URI = process.env.MONGODB_URI || 'mongodb://root:example@localhost:27017/';
const mongoClient = new MongoClient(MONGO_URI);

// Funció per crear un PDF amb una llista de títols
async function crearInforme(destinacio, llistaTitols) {
  const pdf = new PDFDocument();
  pdf.pipe(fs.createWriteStream(destinacio));

  pdf.fontSize(16).text('Informe de resultats', { align: 'center' });
  pdf.moveDown();

  llistaTitols.forEach((titol, idx) => {
    pdf.fontSize(12).text(`${idx + 1}. ${titol}`);
  });

  pdf.end();
}

// Funció principal del procés
async function executarConsulta() {
  try {
    await mongoClient.connect();
    console.log('[INFO] Connexió a MongoDB establerta');

    const db = mongoClient.db('questions_db');
    const preguntes = db.collection('questions');

    // Consulta 1: ViewCount superior a la mitjana
    const mitjanaResultat = await preguntes.aggregate([
      { $group: { _id: null, valorMig: { $avg: '$question.ViewCount' } } }
    ]).toArray();

    const mitjana = mitjanaResultat[0]?.valorMig || 0;

    const resultats1 = await preguntes.find({
      'question.ViewCount': { $gt: mitjana }
    }).toArray();

    console.log(`[INFO] S'han trobat ${resultats1.length} preguntes amb més visualitzacions que la mitjana`);

    const titols1 = resultats1.map(doc => doc.question.Title);

    // Crear directori de sortida si no existeix
    const carpetaSortida = path.resolve(__dirname, '../../data/out');
    if (!fs.existsSync(carpetaSortida)) {
      fs.mkdirSync(carpetaSortida, { recursive: true });
    }

    const rutaPDF1 = path.join(carpetaSortida, 'informe1.pdf');
    await crearInforme(rutaPDF1, titols1);
    console.log('[INFO] informe1.pdf generat correctament');

    // Consulta 2: coincidència amb paraules clau
    const claus = ['pug', 'wig', 'yak', 'nap', 'jig', 'mug', 'zap', 'gag', 'oaf', 'elf'];
    const expressio = new RegExp(claus.join('|'), 'i');

    const resultats2 = await preguntes.find({
      'question.Title': { $regex: expressio }
    }).toArray();

    console.log(`[INFO] S'han trobat ${resultats2.length} preguntes amb coincidències de paraules clau`);

    const titols2 = resultats2.map(doc => doc.question.Title);

    const rutaPDF2 = path.join(carpetaSortida, 'informe2.pdf');
    await crearInforme(rutaPDF2, titols2);
    console.log('[INFO] informe2.pdf creat amb èxit');

  } catch (err) {
    console.error('[ERROR] S\'ha produït un problema durant l\'execució:', err);
  } finally {
    await mongoClient.close();
    console.log('[INFO] Connexió amb MongoDB tancada');
  }
}

executarConsulta();
