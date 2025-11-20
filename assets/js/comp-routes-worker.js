// Worker builds inverted indexes and answers queries by set ops.
const toSet = (arr) => {
  return new Set(arr && arr.length ? arr : []);
};

let routeMeta = new Map(); // routeKey -> route object
let allRouteKeys = new Set(); // all route keys
let specIndex = new Map(); // specId (string) -> Set(routeKey)
let spellIndex = new Map(); // spellId (number) -> Set(routeKey)
let npcIndex = new Map(); // npcId (number) -> Set(routeKey)
let dungeonIndex = new Map(); // dungeonSlug -> Set(routeKey)

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

  for (const [specKey, info] of Object.entries(compRoutes)) {
    const routeKey = info.route_key;
    if (!routeKey) continue;
    routeMeta.set(routeKey, info);
    allRouteKeys.add(routeKey);

    const specs =
      info.specs ||
      (specKey === "unknown"
        ? []
        : specKey
            .split(",")
            .filter(Boolean)
            .map((s) => String(s)));
    for (const s of specs) {
      addToIndex(specIndex, String(s), routeKey);
    }

    const spells = info.spells || [];
    for (const sp of spells) addToIndex(spellIndex, String(sp), routeKey);

    const npcs = info.npcs || [];
    for (const n of npcs) addToIndex(npcIndex, String(n), routeKey);

    const dungeon = String(info.dungeon || "");
    if (dungeon) addToIndex(dungeonIndex, dungeon, routeKey);
  }
}

function intersectSets(sets) {
  if (!sets || sets.length === 0) return new Set(allRouteKeys);
  sets.sort((a, b) => a.size - b.size);
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
    const {
      dungeons = [],
      specs = [],
      spells = [],
      npcInclude = [],
      npcExclude = [],
      page = 1,
      pageSize = 50,
    } = msg.payload || {};

    let candidateSets = [];

    if (dungeons && dungeons.length > 0) {
      const ds = [];
      for (const d of dungeons) {
        if (dungeonIndex.has(String(d))) ds.push(dungeonIndex.get(String(d)));
      }
      if (ds.length === 0) {
        self.postMessage({
          cmd: "result",
          total: 0,
          page,
          pageSize,
          results: [],
        });
        return;
      }
      candidateSets.push(unionSets(ds));
    }

    if (specs && specs.length > 0) {
      const ss = [];
      for (const s of specs) {
        if (specIndex.has(String(s))) ss.push(specIndex.get(String(s)));
        else {
          self.postMessage({
            cmd: "result",
            total: 0,
            page,
            pageSize,
            results: [],
          });
          return;
        }
      }
      candidateSets.push(intersectSets(ss));
    }

    if (spells && spells.length > 0) {
      const ps = [];
      for (const p of spells) {
        if (spellIndex.has(String(p))) ps.push(spellIndex.get(String(p)));
      }
      if (ps.length === 0) {
        self.postMessage({
          cmd: "result",
          total: 0,
          page,
          pageSize,
          results: [],
        });
        return;
      }
      candidateSets.push(unionSets(ps));
    }

    if (npcInclude && npcInclude.length > 0) {
      const nis = [];
      for (const n of npcInclude) {
        if (npcIndex.has(String(n))) nis.push(npcIndex.get(String(n)));
      }
      if (nis.length === 0) {
        self.postMessage({
          cmd: "result",
          total: 0,
          page,
          pageSize,
          results: [],
        });
        return;
      }
      candidateSets.push(unionSets(nis));
    }

    let matchesSet =
      candidateSets.length > 0
        ? intersectSets(candidateSets)
        : new Set(allRouteKeys);

    if (npcExclude && npcExclude.length > 0) {
      for (const ex of npcExclude) {
        const s = npcIndex.get(String(ex));
        if (!s) continue;
        for (const rk of s) matchesSet.delete(rk);
      }
    }

    const matchedRoutes = Array.from(matchesSet)
      .map((rk) => routeMeta.get(rk))
      .filter(Boolean);

    matchedRoutes.sort((a, b) => {
      if ((a.level || 0) !== (b.level || 0))
        return (b.level || 0) - (a.level || 0);
      if ((a.duration || 0) !== (b.duration || 0))
        return (a.duration || 0) - (b.duration || 0);
      if ((a.dungeon || "") !== (b.dungeon || ""))
        return String(a.dungeon).localeCompare(String(b.dungeon));
      return (a.timestamp || 0) - (b.timestamp || 0);
    });

    const total = matchedRoutes.length;
    const start = (page - 1) * pageSize;
    const results = matchedRoutes.slice(start, start + pageSize);

    self.postMessage({ cmd: "result", total, page, pageSize, results });
  }
};
