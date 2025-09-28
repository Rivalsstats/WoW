// compRoutesWorker.js
// Worker builds inverted indexes and answers queries by set ops.
// Messages:
//  { cmd: "build", payload: compRoutes }  -> builds indexes
//  { cmd: "query", payload: { dungeons, specs, spells, npcInclude, npcExclude, page, pageSize } } -> returns matched route list

// Helper: create Set from iterable
const toSet = (arr) => {
  return new Set((arr && arr.length) ? arr : []);
};

let routeMeta = new Map();     // routeKey -> route object
let allRouteKeys = new Set();  // all route keys
let specIndex = new Map();     // specId (string) -> Set(routeKey)
let spellIndex = new Map();    // spellId (number) -> Set(routeKey)
let npcIndex = new Map();      // npcId (number) -> Set(routeKey)
let dungeonIndex = new Map();  // dungeonSlug -> Set(routeKey)

function addToIndex(indexMap, key, routeKey) {
  if (!indexMap.has(key)) indexMap.set(key, new Set());
  indexMap.get(key).add(routeKey);
}

function buildIndexes(compRoutes) {
  routeMeta.clear();
  allRouteKeys.clear();
  specIndex.clear();
  spellIndex.clear();
  npcIndex.clear();
  dungeonIndex.clear();

  // compRoutes is an object: specKey -> info, but each info has route_key
  for (const [specKey, info] of Object.entries(compRoutes)) {
    const routeKey = info.route_key;
    if (!routeKey) continue;
    routeMeta.set(routeKey, info);
    allRouteKeys.add(routeKey);

    // specs
    const specs = info.specs || (specKey === 'unknown' ? [] : specKey.split(',').filter(Boolean).map(s=>String(s)));
    for (const s of specs) {
      addToIndex(specIndex, String(s), routeKey);
    }

    // spells
    const spells = info.spells || [];
    for (const sp of spells) addToIndex(spellIndex, String(sp), routeKey);

    // npcs
    const npcs = info.npcs || [];
    for (const n of npcs) addToIndex(npcIndex, String(n), routeKey);

    // dungeon
    const dungeon = String(info.dungeon || "");
    if (dungeon) addToIndex(dungeonIndex, dungeon, routeKey);
  }
}

// set intersection helper (smallest-first)
function intersectSets(sets) {
  if (!sets || sets.length === 0) return new Set(allRouteKeys); // nothing to intersect -> all
  // sort by size asc for faster intersection
  sets.sort((a,b) => a.size - b.size);
  let out = new Set(sets[0]);
  for (let i = 1; i < sets.length; ++i) {
    const s = sets[i];
    for (const v of out) {
      if (!s.has(v)) out.delete(v);
    }
    if (out.size === 0) break;
  }
  return out;
}

// union helper
function unionSets(sets) {
  const out = new Set();
  for (const s of sets) {
    for (const v of s) out.add(v);
  }
  return out;
}

self.onmessage = (ev) => {
  const msg = ev.data;
  if (!msg || !msg.cmd) return;
  if (msg.cmd === "build") {
    try {
      buildIndexes(msg.payload || {});
      self.postMessage({ cmd: "built", total: allRouteKeys.size });
    } catch (e) {
      self.postMessage({ cmd: "error", error: String(e) });
    }
    return;
  }

  if (msg.cmd === "query") {
    const { dungeons = [], specs = [], spells = [], npcInclude = [], npcExclude = [], page = 1, pageSize = 50 } = msg.payload || {};

    // Start with "all" or with dungeon filter (union of dungeon sets)
    let candidateSets = [];

    if (dungeons && dungeons.length > 0) {
      const ds = [];
      for (const d of dungeons) {
        if (dungeonIndex.has(String(d))) ds.push(dungeonIndex.get(String(d)));
      }
      if (ds.length === 0) {
        // no route matches this dungeon list
        self.postMessage({ cmd: "result", total: 0, page, pageSize, results: [] });
        return;
      }
      candidateSets.push(unionSets(ds)); // routes in any selected dungeon
    }

    // specs: user expects routes to contain ALL chosen specs -> intersect spec sets
    if (specs && specs.length > 0) {
      const ss = [];
      for (const s of specs) {
        if (specIndex.has(String(s))) ss.push(specIndex.get(String(s)));
        else {
          // missing spec => no results
          self.postMessage({ cmd: "result", total: 0, page, pageSize, results: [] });
          return;
        }
      }
      candidateSets.push(intersectSets(ss) /* this returns Set */);
    }

    // spellsWanted: must include at least one -> union of spell sets, then intersect with candidate
    if (spells && spells.length > 0) {
      const ps = [];
      for (const p of spells) {
        if (spellIndex.has(String(p))) ps.push(spellIndex.get(String(p)));
      }
      if (ps.length === 0) {
        self.postMessage({ cmd: "result", total: 0, page, pageSize, results: [] });
        return;
      }
      candidateSets.push(unionSets(ps));
    }

    // npcInclude: at least one -> union and intersect
    if (npcInclude && npcInclude.length > 0) {
      const nis = [];
      for (const n of npcInclude) {
        if (npcIndex.has(String(n))) nis.push(npcIndex.get(String(n)));
      }
      if (nis.length === 0) {
        self.postMessage({ cmd: "result", total: 0, page, pageSize, results: [] });
        return;
      }
      candidateSets.push(unionSets(nis));
    }

    // compute intersection of candidate sets (if none -> all routes)
    let matchesSet = (candidateSets.length > 0) ? intersectSets(candidateSets) : new Set(allRouteKeys);

    // apply npcExclude: remove any route that contains an excluded npc
    if (npcExclude && npcExclude.length > 0) {
      for (const ex of npcExclude) {
        const s = npcIndex.get(String(ex));
        if (!s) continue;
        for (const rk of s) matchesSet.delete(rk);
      }
    }

    // turn matchesSet into sorted array of route objects (sort by level desc, duration asc, timestamp asc)
    const matchedRoutes = Array.from(matchesSet).map(rk => routeMeta.get(rk)).filter(Boolean);

    matchedRoutes.sort((a,b) => {
      if ((a.level || 0) !== (b.level || 0)) return (b.level || 0) - (a.level || 0);
      if ((a.duration || 0) !== (b.duration || 0)) return (a.duration || 0) - (b.duration || 0);
      if ((a.dungeon || "") !== (b.dungeon || "")) return String(a.dungeon).localeCompare(String(b.dungeon));
      return (a.timestamp || 0) - (b.timestamp || 0);
    });

    const total = matchedRoutes.length;
    const start = (page - 1) * pageSize;
    const results = matchedRoutes.slice(start, start + pageSize);

    self.postMessage({ cmd: "result", total, page, pageSize, results });
  }
};
