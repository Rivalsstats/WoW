-- Mythistone.aggregated_bonus_lists definition

CREATE TABLE `aggregated_bonus_lists` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` varchar(100) NOT NULL,
  `bonus_list` text NOT NULL,
  `bonus_hash` char(32) GENERATED ALWAYS AS (md5(`bonus_list`)) STORED NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`,`bonus_hash`),
  KEY `idx_agg_summary_spec_season_item` (`spec_id`,`season`,`item_id`),
  KEY `idx_agg_summary_bonus_hash` (`bonus_hash`)
) /*!50100 TABLESPACE `aggregated_bonus_lists` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_character_stats definition

CREATE TABLE `aggregated_character_stats` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `stat` varchar(100) NOT NULL,
  `avg_percent` double unsigned DEFAULT NULL,
  `avg_raw` bigint unsigned DEFAULT NULL,
  `min_raw` bigint unsigned DEFAULT NULL,
  `max_raw` bigint unsigned DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_dungeon_comps definition

CREATE TABLE `aggregated_dungeon_comps` (
  `dungeon_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `season` int NOT NULL,
  `comp` varchar(255) NOT NULL,
  `run_count` bigint unsigned NOT NULL,
  PRIMARY KEY (`dungeon_id`,`season`,`comp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_dungeon_global_specs definition

CREATE TABLE `aggregated_dungeon_global_specs` (
  `season` int NOT NULL,
  `spec_id` int NOT NULL,
  `run_count` bigint unsigned NOT NULL,
  PRIMARY KEY (`season`,`spec_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_dungeon_specs definition

CREATE TABLE `aggregated_dungeon_specs` (
  `dungeon_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `season` int NOT NULL,
  `spec_id` int NOT NULL,
  `run_count` bigint unsigned NOT NULL,
  PRIMARY KEY (`dungeon_id`,`season`,`spec_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_spec definition

CREATE TABLE `aggregated_spec` (
  `spec_id` int unsigned NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` varchar(20) NOT NULL,
  `run_count` bigint unsigned NOT NULL DEFAULT '0',
  `hero_talent_id` int unsigned NOT NULL DEFAULT '0',
  `season` int unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`keystone_level`,`spec_id`,`upgrade_tier`,`hero_talent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.crafted_item_ids definition

CREATE TABLE `crafted_item_ids` (
  `item_id` int NOT NULL,
  PRIMARY KEY (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.dungeon_data definition

CREATE TABLE `dungeon_data` (
  `dungeon_id` varchar(100) NOT NULL,
  `slug` varchar(100) NOT NULL,
  `name_en_us` varchar(100) NOT NULL,
  `upgrade_1_duration` bigint NOT NULL,
  `upgrade_2_duration` bigint NOT NULL,
  `upgrade_3_duration` bigint NOT NULL,
  PRIMARY KEY (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.embellishments definition

CREATE TABLE `embellishments` (
  `bonus_id` int NOT NULL,
  `item_id` int NOT NULL,
  PRIMARY KEY (`bonus_id`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_bonus_lists definition

CREATE TABLE `global_aggregated_bonus_lists` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` varchar(100) NOT NULL,
  `bonus_list` text NOT NULL,
  `bonus_hash` char(32) GENERATED ALWAYS AS (md5(`bonus_list`)) STORED NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`,`bonus_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_crafted_items definition

CREATE TABLE `global_aggregated_crafted_items` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_embellishments definition

CREATE TABLE `global_aggregated_embellishments` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_enchantments_slot_group definition

CREATE TABLE `global_aggregated_enchantments_slot_group` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `slot_group` varchar(100) NOT NULL,
  `enchantment_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`slot_group`,`enchantment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_equipment definition

CREATE TABLE `global_aggregated_equipment` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` varchar(100) NOT NULL,
  `slot` varchar(100) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`,`slot`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_hero_talent_overview definition

CREATE TABLE `global_aggregated_hero_talent_overview` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `hero_talent_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`hero_talent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_item_sockets definition

CREATE TABLE `global_aggregated_item_sockets` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` varchar(100) NOT NULL,
  `socket_item_id` varchar(100) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`,`socket_item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_loadout_data definition

CREATE TABLE `global_aggregated_loadout_data` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `hero_talent_id` int NOT NULL,
  `loadout` varchar(255) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`hero_talent_id`,`loadout`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_missives definition

CREATE TABLE `global_aggregated_missives` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.hunter_pets definition

CREATE TABLE `hunter_pets` (
  `member` int unsigned NOT NULL,
  `creature_id` bigint unsigned NOT NULL,
  PRIMARY KEY (`creature_id`,`member`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.members definition

CREATE TABLE `members` (
  `member` int unsigned NOT NULL AUTO_INCREMENT,
  `spec_id` int NOT NULL,
  `loadout` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `hero_talent_id` int DEFAULT NULL,
  PRIMARY KEY (`member`)
) /*!50100 TABLESPACE `members` */ ENGINE=InnoDB AUTO_INCREMENT=90474359 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.missives definition

CREATE TABLE `missives` (
  `bonus_id` int unsigned NOT NULL,
  `item_id` int unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.route_data definition

CREATE TABLE `route_data` (
  `rio_run_id` bigint unsigned NOT NULL,
  `mapping_version` int NOT NULL,
  `enemy_forces` int NOT NULL,
  `timestamp` bigint unsigned NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `duration` int unsigned NOT NULL,
  `dungeon_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `route_key` varchar(100) NOT NULL,
  PRIMARY KEY (`route_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.season_periods definition

CREATE TABLE `season_periods` (
  `region` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `period_id` int unsigned NOT NULL,
  `start_timestamp` bigint unsigned NOT NULL,
  `end_timestamp` bigint unsigned NOT NULL,
  `season` int NOT NULL,
  PRIMARY KEY (`region`,`period_id`,`season`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.slot_group_map definition

CREATE TABLE `slot_group_map` (
  `slot` varchar(100) NOT NULL,
  `slot_group` varchar(100) NOT NULL,
  PRIMARY KEY (`slot`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.summary_meta definition

CREATE TABLE `summary_meta` (
  `name` varchar(100) NOT NULL,
  `last_run_id` bigint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_class_talent definition

CREATE TABLE `aggregated_class_talent` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `dungeon_id` varchar(100) NOT NULL,
  `hero_talent_id` int NOT NULL,
  `talent_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`talent_id`,`hero_talent_id`),
  KEY `dungeon_id` (`dungeon_id`),
  CONSTRAINT `aggregated_class_talent_ibfk_1` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_crafted_items definition

CREATE TABLE `aggregated_crafted_items` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `dungeon_id` varchar(100) NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` enum('1','2','3','depleted') NOT NULL,
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `item_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`keystone_level`,`upgrade_tier`,`hero_talent_id`,`item_id`),
  KEY `idx_agg_crafted_spec_season_item` (`spec_id`,`season`,`item_id`),
  KEY `aggregated_crafted_items_fk_dd` (`dungeon_id`),
  CONSTRAINT `aggregated_crafted_items_fk_dd` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_embellishments definition

CREATE TABLE `aggregated_embellishments` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `dungeon_id` varchar(100) NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` enum('1','2','3','depleted') NOT NULL,
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `item_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`keystone_level`,`upgrade_tier`,`hero_talent_id`,`item_id`),
  KEY `idx_agg_emb_spec_season_item` (`spec_id`,`season`,`item_id`),
  KEY `aggregated_embellishments_fk_dd` (`dungeon_id`),
  CONSTRAINT `aggregated_embellishments_fk_dd` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_enchantments_slot_group definition

CREATE TABLE `aggregated_enchantments_slot_group` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `dungeon_id` varchar(100) NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` enum('1','2','3','depleted') NOT NULL,
  `hero_talent_id` int NOT NULL,
  `slot_group` varchar(100) NOT NULL,
  `enchantment_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`keystone_level`,`upgrade_tier`,`hero_talent_id`,`slot_group`,`enchantment_id`),
  KEY `dungeon_id_idx` (`dungeon_id`),
  KEY `enchantment_id_idx` (`enchantment_id`),
  CONSTRAINT `aggregated_enchantments_slot_group_fk_dd` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_equipment definition

CREATE TABLE `aggregated_equipment` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `dungeon_id` varchar(100) NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` enum('1','2','3','depleted') NOT NULL,
  `hero_talent_id` int NOT NULL,
  `item_id` varchar(100) NOT NULL,
  `slot` varchar(100) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`keystone_level`,`upgrade_tier`,`hero_talent_id`,`item_id`,`slot`),
  KEY `dungeon_id_idx` (`dungeon_id`),
  KEY `item_id_idx` (`item_id`),
  CONSTRAINT `aggregated_equipment_fk_dd` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_hero_talent definition

CREATE TABLE `aggregated_hero_talent` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `dungeon_id` varchar(100) NOT NULL,
  `hero_talent_id` int NOT NULL,
  `talent_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`hero_talent_id`,`talent_id`),
  KEY `dungeon_id` (`dungeon_id`),
  CONSTRAINT `aggregated_hero_talent_ibfk_1` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_item_sockets definition

CREATE TABLE `aggregated_item_sockets` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `dungeon_id` varchar(100) NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` enum('1','2','3','depleted') NOT NULL,
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `item_id` varchar(100) NOT NULL,
  `socket_item_id` varchar(100) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`keystone_level`,`upgrade_tier`,`hero_talent_id`,`item_id`,`socket_item_id`),
  KEY `idx_agg_sockets_spec_season_item` (`spec_id`,`season`,`item_id`),
  KEY `aggregated_item_sockets_fk_dd` (`dungeon_id`),
  CONSTRAINT `aggregated_item_sockets_fk_dd` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_loadout_data definition

CREATE TABLE `aggregated_loadout_data` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `dungeon_id` varchar(100) NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` enum('1','2','3','depleted') NOT NULL,
  `hero_talent_id` int DEFAULT NULL,
  `loadout` varchar(255) DEFAULT NULL,
  `hero_talent_id_key` int GENERATED ALWAYS AS (ifnull(`hero_talent_id`,0)) STORED NOT NULL,
  `loadout_key` varchar(255) GENERATED ALWAYS AS (ifnull(`loadout`,_utf8mb4'<NULL>')) STORED NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`keystone_level`,`upgrade_tier`,`hero_talent_id_key`,`loadout_key`),
  KEY `idx_dungeon` (`dungeon_id`),
  KEY `idx_spec` (`spec_id`),
  CONSTRAINT `fk_agg_loadout_dungeon` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_missives definition

CREATE TABLE `aggregated_missives` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `dungeon_id` varchar(100) NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` enum('1','2','3','depleted') NOT NULL,
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `item_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`keystone_level`,`upgrade_tier`,`hero_talent_id`,`item_id`),
  KEY `idx_agg_missives_spec_season_item` (`spec_id`,`season`,`item_id`),
  KEY `aggregated_missives_fk_dd` (`dungeon_id`),
  CONSTRAINT `aggregated_missives_fk_dd` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_spec_talent definition

CREATE TABLE `aggregated_spec_talent` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `dungeon_id` varchar(100) NOT NULL,
  `hero_talent_id` int NOT NULL,
  `talent_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`hero_talent_id`,`talent_id`),
  KEY `dungeon_id` (`dungeon_id`),
  CONSTRAINT `aggregated_spec_talent_ibfk_1` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.character_stats definition

CREATE TABLE `character_stats` (
  `member` int unsigned NOT NULL,
  `stat` varchar(100) NOT NULL,
  `percent` double unsigned DEFAULT NULL,
  `raw` bigint unsigned NOT NULL,
  PRIMARY KEY (`stat`,`member`),
  KEY `character_stats_members_FK` (`member`),
  CONSTRAINT `character_stats_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) /*!50100 TABLESPACE `character_stats` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.class_talents definition

CREATE TABLE `class_talents` (
  `member` int unsigned NOT NULL,
  `talent_id` int unsigned NOT NULL,
  `rank` int NOT NULL,
  PRIMARY KEY (`member`,`talent_id`),
  CONSTRAINT `class_talents_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) /*!50100 TABLESPACE `vol_class_talents` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
) /*!50100 TABLESPACE `equipments` */ ENGINE=InnoDB AUTO_INCREMENT=209345469 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.hero_talents definition

CREATE TABLE `hero_talents` (
  `member` int unsigned NOT NULL,
  `talent_id` int unsigned NOT NULL,
  `rank` int NOT NULL,
  PRIMARY KEY (`member`,`talent_id`),
  CONSTRAINT `hero_talents_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.route_pulls definition

CREATE TABLE `route_pulls` (
  `route_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `pull_id` int unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`pull_id`,`route_key`),
  KEY `route_pulls_route_data_FK` (`route_key`),
  CONSTRAINT `route_pulls_route_data_FK` FOREIGN KEY (`route_key`) REFERENCES `route_data` (`route_key`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=214004 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.route_specs definition

CREATE TABLE `route_specs` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `spec_id` int NOT NULL,
  `route_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_route_key` (`route_key`),
  CONSTRAINT `route_specs_route_data_FK` FOREIGN KEY (`route_key`) REFERENCES `route_data` (`route_key`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=73435 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
  UNIQUE KEY `runs_unique` (`dungeon_id`,`keystone_level`,`duration`,`timestamp`,`faction`,`region`,`season`),
  CONSTRAINT `runs_dungeon_data_FK` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
) /*!50100 TABLESPACE `ts_runs` */ ENGINE=InnoDB AUTO_INCREMENT=46449868 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.sockets definition

CREATE TABLE `sockets` (
  `socket_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `socket_item_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `equipment_id` int unsigned NOT NULL,
  `socket_id_pk` bigint unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`socket_id_pk`),
  KEY `sockets_equipment_FK` (`equipment_id`),
  CONSTRAINT `sockets_equipment_FK` FOREIGN KEY (`equipment_id`) REFERENCES `equipment` (`equipment_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=6031911 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.spec_talents definition

CREATE TABLE `spec_talents` (
  `talent_id` int unsigned NOT NULL,
  `member` int unsigned NOT NULL,
  `rank` int NOT NULL,
  PRIMARY KEY (`talent_id`,`member`),
  KEY `spec_talents_run_members_FK` (`member`),
  CONSTRAINT `spec_talents_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) /*!50100 TABLESPACE `spec_talents_vol` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.bonus_ids definition

CREATE TABLE `bonus_ids` (
  `equipment_id` int unsigned NOT NULL,
  `bonus_id` int unsigned NOT NULL,
  PRIMARY KEY (`equipment_id`,`bonus_id`),
  CONSTRAINT `bonus_ids_equipment_FK` FOREIGN KEY (`equipment_id`) REFERENCES `equipment` (`equipment_id`) ON DELETE CASCADE ON UPDATE CASCADE
) /*!50100 TABLESPACE `vol_bonus_ids` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.enchantments definition

CREATE TABLE `enchantments` (
  `enchantment_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `equipment_id` int unsigned NOT NULL,
  `enchantment_id_pk` bigint unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`enchantment_id_pk`),
  KEY `enchantments_equipment_FK` (`equipment_id`),
  CONSTRAINT `enchantments_equipment_FK` FOREIGN KEY (`equipment_id`) REFERENCES `equipment` (`equipment_id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB AUTO_INCREMENT=14404633 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.pull_enemies definition

CREATE TABLE `pull_enemies` (
  `route_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `npc_id` int unsigned NOT NULL,
  `pull_id` int unsigned NOT NULL,
  `count` smallint unsigned NOT NULL,
  PRIMARY KEY (`npc_id`,`pull_id`,`route_key`),
  KEY `pull_enemies_route_data_FK` (`route_key`),
  KEY `pull_enemies_route_pulls_FK` (`pull_id`,`route_key`),
  CONSTRAINT `pull_enemies_route_pulls_FK` FOREIGN KEY (`pull_id`, `route_key`) REFERENCES `route_pulls` (`pull_id`, `route_key`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.pull_spells definition

CREATE TABLE `pull_spells` (
  `route_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `spell_id` int unsigned NOT NULL,
  `pull_id` int unsigned NOT NULL,
  PRIMARY KEY (`route_key`,`spell_id`,`pull_id`),
  KEY `pull_spells_route_pulls_FK` (`pull_id`,`route_key`),
  CONSTRAINT `pull_spells_route_pulls_FK` FOREIGN KEY (`pull_id`, `route_key`) REFERENCES `route_pulls` (`pull_id`, `route_key`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.run_members definition

CREATE TABLE `run_members` (
  `member` int unsigned NOT NULL,
  `run_id` int unsigned NOT NULL,
  PRIMARY KEY (`member`,`run_id`),
  KEY `run_members_runs_FK` (`run_id`),
  CONSTRAINT `run_members_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `run_members_runs_FK` FOREIGN KEY (`run_id`) REFERENCES `runs` (`run_id`) ON DELETE CASCADE ON UPDATE RESTRICT
) /*!50100 TABLESPACE `ts_run_members` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;