use bigdata_store;

// --- 1) Crear colecciones implícitamente y generar productos (100) ---
const categories = ["Electrónica", "Hogar", "Ropa", "Deportes", "Juguetes", "Salud", "Libros"];
const tagsPool = ["nuevo","oferta","popular","eco","importado","recomendado","edición_limitada"];

function randInt(min, max) { return Math.floor(Math.random()*(max-min+1))+min; }
function randFloat(min, max, dec=2) { return Math.round((Math.random()*(max-min)+min)*Math.pow(10,dec))/Math.pow(10,dec); }
function pick(arr){ return arr[Math.floor(Math.random()*arr.length)]; }

let products = [];
for(let i=1;i<=100;i++){
  const cat = pick(categories);
  const price = randFloat(5, 1000);
  const stock = randInt(0,200);
  const rating = randFloat(2.5,5,2);
  const nTags = randInt(1,3);
  const tags = [];
  for(let t=0;t<nTags;t++){ tags.push(pick(tagsPool)); }
  products.push({
    name: `Producto ${i} - ${cat}`,
    category: cat,
    price: price,
    stock: stock,
    rating: rating,
    tags: Array.from(new Set(tags)),
    attributes: {
      color: pick(["negro","blanco","rojo","azul","verde","multicolor"]),
      weight_g: randInt(50,3000)
    },
    createdAt: new Date(2024, randInt(0,11), randInt(1,28))
  });
}

const prodResult = db.products.insertMany(products);
const prodIds = Object.values(prodResult.insertedIds); // array de ObjectId de productos

// --- 2) Generar usuarios (30) ---
let users = [];
for(let i=1;i<=30;i++){
  users.push({
    name: `Usuario ${i}`,
    email: `usuario${i}@ejemplo.com`,
    signupDate: new Date(2023 + randInt(0,2), randInt(0,11), randInt(1,28)),
    address: { city: pick(["Bogotá","Medellín","Cali","Barranquilla","Cartagena"]), country: "Colombia" }
  });
}
const userResult = db.users.insertMany(users);
const userIds = Object.values(userResult.insertedIds);

// --- 3) Generar orders (200) ---
let orders = [];
for(let i=1;i<=200;i++){
  const userId = pick(userIds);
  const nItems = randInt(1,5);
  let items = [];
  let total = 0;
  for(let j=0;j<nItems;j++){
    const pId = pick(prodIds);
    const quantity = randInt(1,4);
    // obtener precio actual del producto
    const prod = db.products.findOne({_id: pId});
    const priceSnapshot = prod ? prod.price : randFloat(10,200);
    items.push({ productId: pId, quantity: quantity, price: priceSnapshot });
    total += priceSnapshot * quantity;
  }
  const status = pick(["paid","shipped","cancelled"]);
  orders.push({
    userId: userId,
    items: items,
    total: Math.round(total*100)/100,
    status: status,
    orderDate: new Date(2024, randInt(0,11), randInt(1,28))
  });
}
db.orders.insertMany(orders);

// --- 4) Índices recomendados ---
db.products.createIndex({ category: 1 });
db.products.createIndex({ price: 1 });
db.products.createIndex({ name: "text", tags: "text" }); // búsqueda textual básica
db.users.createIndex({ email: 1 }, { unique: true });
db.orders.createIndex({ orderDate: 1 });
db.orders.createIndex({ "items.productId": 1 });

print("Inserción completada: productos:", prodIds.length, "usuarios:", userIds.length, "orders:", db.orders.countDocuments());


4) Consultas básicas (CRUD)
Inserción (ya está en el script: insertMany). Ejemplo de insertOne:
db.products.insertOne({
  name: "Producto Especial",
  category: "Electrónica",
  price: 249.99,
  stock: 50,
  rating: 4.7,
  tags: ["nuevo","recomendado"],
  attributes: { color: "negro", weight_g: 450 },
  createdAt: new Date()
});

Selección (find)

Productos con precio entre 50 y 200:

db.products.find({ price: { $gt: 50, $lt: 200 } }).sort({ price: 1 }).limit(10).pretty();


Explicación: $gt y $lt filtran por rango; se ordena por precio ascendente.

Productos cuyo nombre contiene “Especial” (búsqueda de texto):

db.products.find({ $text: { $search: "Especial" } });


Productos en categorías Ropa o Deportes:

db.products.find({ category: { $in: ["Ropa","Deportes"] } });

Actualización (update)

Reducir stock al vender 1 unidad:

db.products.updateOne(
  { _id: ObjectId("ID_DEL_PRODUCTO") },
  { $inc: { stock: -1 }, $set: { lastSold: new Date() } }
);


Explicación: $inc decrementa stock; $set registra fecha venta.

Actualizar múltiples documentos:

db.products.updateMany(
  { stock: { $lt: 5 } },
  { $set: { "tags": ["oferta","reponer"] } }
);

Eliminación (delete)

Eliminar productos sin stock:

db.products.deleteMany({ stock: 0 });


Eliminar orden cancelada y vieja:

db.orders.deleteMany({ status: "cancelled", orderDate: { $lt: new Date(2024,0,1) } });

5) Consultas con filtros y operadores — ejemplos útiles

$or, $and:

db.products.find({ $or: [ { price: { $lt: 20 } }, { tags: "oferta" } ] });


$regex (búsqueda parcial, case-insensitive):

db.products.find({ name: { $regex: "producto", $options: "i" } });


$elemMatch (filtrar por item en arreglo):

db.orders.find({ items: { $elemMatch: { productId: ObjectId("..."), quantity: { $gte: 2 } } } });


$exists (campos opcionales):

db.products.find({ "attributes.weight_g": { $exists: true } });

6) Consultas de agregación (estadísticas) — con explicaciones y análisis

Usaremos el framework de agregación (db.collection.aggregate([...])).

A) Conteo de productos por categoría
db.products.aggregate([
  { $group: { _id: "$category", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
]);


Qué devuelve: para cada categoría, el número de productos.
Análisis: permite identificar qué categorías tienen más oferta en el catálogo; útil para inventario y decisiones de marketing.

B) Precio promedio por categoría
db.products.aggregate([
  { $group: { _id: "$category", avgPrice: { $avg: "$price" }, minPrice: { $min: "$price" }, maxPrice: { $max: "$price" } } },
  { $sort: { avgPrice: -1 } }
]);


Qué devuelve: promedio, mínimo y máximo del precio por categoría.
Análisis: muestra la dispersión de precios por categoría; por ejemplo, Electrónica probablemente tenga avgPrice mayor que Juguetes.

C) Top 5 productos más vendidos (por cantidad) — usando orders
db.orders.aggregate([
  { $unwind: "$items" },
  { $group: { _id: "$items.productId", totalQuantity: { $sum: "$items.quantity" } } },
  { $sort: { totalQuantity: -1 } },
  { $limit: 5 },
  { $lookup: {
      from: "products",
      localField: "_id",
      foreignField: "_id",
      as: "product"
    }
  },
  { $unwind: "$product" },
  { $project: { productName: "$product.name", totalQuantity: 1, category: "$product.category" } }
]);


Qué devuelve: los 5 productos con mayor cantidad vendida, con su nombre y categoría.
Análisis: identifica los best-sellers; si aparecen productos con poco stock, planificar reposición.

D) Total de ventas por mes (agregación por mes)
db.orders.aggregate([
  { $match: { status: "paid" } },
  { $project: { total: 1, year: { $year: "$orderDate" }, month: { $month: "$orderDate" } } },
  { $group: { _id: { year: "$year", month: "$month" }, monthlySales: { $sum: "$total" }, ordersCount: { $sum: 1 } } },
  { $sort: { "_id.year": 1, "_id.month": 1 } }
]);


Qué devuelve: ventas totales y número de órdenes pagadas por mes.
Análisis: útil para detectar estacionalidad y meses de mayor facturación.

E) Valor promedio de pedido (Average Order Value — AOV)
db.orders.aggregate([
  { $match: { status: "paid" } },
  { $group: { _id: null, avgOrderValue: { $avg: "$total" }, totalRevenue: { $sum: "$total" }, totalOrders: { $sum: 1 } } }
]);


Qué devuelve: promedio de total por orden, además de ingresos totales y número de órdenes.
Análisis: AOV indica cuánto gasta en promedio un cliente por compra; se usa en estrategias de precios y marketing.
