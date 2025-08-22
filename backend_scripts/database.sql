-- Mythistone.members definition

CREATE TABLE `members` (
  `member` int unsigned NOT NULL AUTO_INCREMENT,
  `spec_id` int NOT NULL,
  `loadout` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `hero_talent_id` int DEFAULT NULL,
  PRIMARY KEY (`member`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.runs definition

CREATE TABLE `runs` (
  `dungeon_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `duration` int unsigned NOT NULL,
  `timestamp` bigint unsigned NOT NULL,
  `faction` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `run_id` int unsigned NOT NULL AUTO_INCREMENT,
  `region` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `season` int NOT NULL,
  PRIMARY KEY (`run_id`),
  UNIQUE KEY `runs_unique` (`dungeon_id`,`keystone_level`,`duration`,`timestamp`,`faction`,`region`,`season`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.class_talents definition

CREATE TABLE `class_talents` (
  `member` int unsigned NOT NULL,
  `talent_id` int unsigned NOT NULL,
  `rank` int NOT NULL,
  PRIMARY KEY (`member`,`talent_id`),
  CONSTRAINT `class_talents_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.equipment definition

CREATE TABLE `equipment` (
  `slot` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `item_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `item_level` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `member` int unsigned NOT NULL,
  `equipment_id` int unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`equipment_id`),
  KEY `equipment_run_members_FK` (`member`),
  CONSTRAINT `equipment_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.hero_talents definition

CREATE TABLE `hero_talents` (
  `member` int unsigned NOT NULL,
  `talent_id` int unsigned NOT NULL,
  `rank` int NOT NULL,
  PRIMARY KEY (`member`,`talent_id`),
  CONSTRAINT `hero_talents_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.run_members definition

CREATE TABLE `run_members` (
  `member` int unsigned NOT NULL,
  `run_id` int unsigned NOT NULL,
  PRIMARY KEY (`member`,`run_id`),
  KEY `run_members_runs_FK` (`run_id`),
  CONSTRAINT `run_members_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `run_members_runs_FK` FOREIGN KEY (`run_id`) REFERENCES `runs` (`run_id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.sockets definition

CREATE TABLE `sockets` (
  `socket_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `socket_item_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `equipment_id` int unsigned NOT NULL,
  `socket_id_pk` smallint unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`socket_id_pk`),
  KEY `sockets_equipment_FK` (`equipment_id`),
  CONSTRAINT `sockets_equipment_FK` FOREIGN KEY (`equipment_id`) REFERENCES `equipment` (`equipment_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.spec_talents definition

CREATE TABLE `spec_talents` (
  `talent_id` int unsigned NOT NULL,
  `member` int unsigned NOT NULL,
  `rank` int NOT NULL,
  PRIMARY KEY (`talent_id`,`member`),
  KEY `spec_talents_run_members_FK` (`member`),
  CONSTRAINT `spec_talents_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.bonus_ids definition

CREATE TABLE `bonus_ids` (
  `equipment_id` int unsigned NOT NULL,
  `bonus_id` int unsigned NOT NULL,
  KEY `bonus_ids_equipment_FK` (`equipment_id`),
  CONSTRAINT `bonus_ids_equipment_FK` FOREIGN KEY (`equipment_id`) REFERENCES `equipment` (`equipment_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.enchantments definition

CREATE TABLE `enchantments` (
  `enchantment_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `equipment_id` int unsigned NOT NULL,
  `enchantment_id_pk` int unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`enchantment_id_pk`),
  KEY `enchantments_equipment_FK` (`equipment_id`),
  CONSTRAINT `enchantments_equipment_FK` FOREIGN KEY (`equipment_id`) REFERENCES `equipment` (`equipment_id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB AUTO_INCREMENT=11084 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- Fetching top 10 items for all slots
WITH counts AS (
  SELECT
    sgm.slot_group,
    eq.item_id,
    COUNT(eq.equipment_id) AS equip_count
  FROM Mythistone.equipment eq
  JOIN Mythistone.members rm  ON rm.`member` = eq.`member`
  JOIN Mythistone.slot_group_map sgm ON sgm.slot = eq.slot
  INNER JOIN Mythistone.run_members runm ON runm.`member` = rm.`member` 
  INNER  JOIN Mythistone.runs r ON r.run_id = runm.run_id 
  WHERE rm.spec_id LIKE '62' AND r.season LIKE '14'
  GROUP BY sgm.slot_group, eq.item_id
)
SELECT slot_group, item_id, equip_count
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY slot_group ORDER BY equip_count DESC) AS rn
  FROM counts
) t
WHERE rn <= 10;

-- Fetch top enchant for slot

WITH counts AS (
  SELECT
    sgm.slot_group,
    e.enchantment_id,
    COUNT(eq.equipment_id) AS equip_count
  FROM Mythistone.enchantments e
  JOIN Mythistone.equipment eq ON eq.equipment_id = e.equipment_id
  JOIN Mythistone.members rm  ON rm.`member` = eq.`member`
  JOIN Mythistone.slot_group_map sgm ON sgm.slot = eq.slot
  INNER JOIN Mythistone.run_members runm ON runm.`member` = rm.`member` 
  INNER  JOIN Mythistone.runs r ON r.run_id = runm.run_id 
  WHERE rm.spec_id LIKE '62' AND r.season LIKE '14' AND sgm.slot_group  LIKE 'FINGER'
  GROUP BY sgm.slot_group, e.enchantment_id
)
SELECT slot_group, enchantment_id, equip_count
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY slot_group ORDER BY equip_count DESC) AS rn
  FROM counts
) t
WHERE rn <= 10;

-- Fetch top enchant for item

WITH counts AS (
  SELECT
    eq.item_id,
    e.enchantment_id,
    COUNT(eq.equipment_id) AS equip_count
  FROM Mythistone.enchantments e
  JOIN Mythistone.equipment eq ON eq.equipment_id = e.equipment_id
  JOIN Mythistone.members rm  ON rm.`member` = eq.`member`
  JOIN Mythistone.slot_group_map sgm ON sgm.slot = eq.slot
  INNER JOIN Mythistone.run_members runm ON runm.`member` = rm.`member` 
  INNER  JOIN Mythistone.runs r ON r.run_id = runm.run_id 
  WHERE rm.spec_id LIKE '62' AND r.season LIKE '14' AND eq.item_id  LIKE 222817
  GROUP BY eq.item_id, e.enchantment_id
)
SELECT enchantment_id, equip_count
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY equip_count DESC) AS rn
  FROM counts
) t
WHERE rn <= 1;

-- fetch top sockets for slot:

WITH counts AS (
  SELECT
    s.socket_item_id ,
    COUNT(eq.equipment_id) AS equip_count
  FROM Mythistone.sockets s 
  JOIN Mythistone.equipment eq ON eq.equipment_id = s.equipment_id
  JOIN Mythistone.members rm  ON rm.`member` = eq.`member`
  INNER JOIN Mythistone.run_members runm ON runm.`member` = rm.`member` 
  INNER  JOIN Mythistone.runs r ON r.run_id = runm.run_id 
  WHERE rm.spec_id LIKE '62' AND r.season LIKE '14'
  GROUP BY s.socket_item_id
)
SELECT socket_item_id, equip_count
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY equip_count DESC) AS rn
  FROM counts
) t
WHERE rn <= 10;

-- fetch top socket for item:

WITH counts AS (
  SELECT
    s.socket_item_id ,
    COUNT(eq.equipment_id) AS equip_count
  FROM Mythistone.sockets s 
  JOIN Mythistone.equipment eq ON eq.equipment_id = s.equipment_id
  JOIN Mythistone.members rm  ON rm.`member` = eq.`member`
  JOIN Mythistone.slot_group_map sgm ON sgm.slot = eq.slot
  INNER JOIN Mythistone.run_members runm ON runm.`member` = rm.`member` 
  INNER  JOIN Mythistone.runs r ON r.run_id = runm.run_id 
  WHERE rm.spec_id LIKE '62' AND r.season LIKE '14' AND eq.item_id  LIKE 222817
  GROUP BY s.socket_item_id
)
SELECT socket_item_id, equip_count
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY equip_count DESC) AS rn
  FROM counts
) t
WHERE rn <= 1;

-- fetch top bonus_ids for item:

WITH bonus_lists AS (
    SELECT
        eq.equipment_id,
        GROUP_CONCAT(DISTINCT b.bonus_id ORDER BY b.bonus_id ASC SEPARATOR ',') AS bonus_list
    FROM Mythistone.equipment eq
    JOIN Mythistone.bonus_ids b 
        ON b.equipment_id = eq.equipment_id
    JOIN Mythistone.members rm  
        ON rm.`member` = eq.`member`
    JOIN Mythistone.slot_group_map sgm 
        ON sgm.slot = eq.slot
    INNER JOIN Mythistone.run_members runm 
        ON runm.`member` = rm.`member`
    INNER JOIN Mythistone.runs r 
        ON r.run_id = runm.run_id
    WHERE rm.spec_id LIKE '62'
      AND r.season LIKE '14'
      AND eq.item_id = 222817
    GROUP BY eq.equipment_id
),
bonus_counts AS (
    SELECT
        bonus_list,
        COUNT(*) AS list_count
    FROM bonus_lists
    GROUP BY bonus_list
)
SELECT bonus_list, list_count
FROM bonus_counts
ORDER BY list_count DESC, bonus_list
LIMIT 1;

