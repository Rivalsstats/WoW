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
  `keystone_level` int unsigned NOT NULL,
  `comp` varchar(255) NOT NULL,
  `timed_runs` bigint unsigned NOT NULL DEFAULT '0',
  `depleted_runs` bigint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`dungeon_id`,`season`,`keystone_level`,`comp`)
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
  `max_keystone_level` int unsigned DEFAULT '0',
  `timed_runs` bigint unsigned DEFAULT '0',
  `depleted_runs` bigint unsigned DEFAULT '0',
  PRIMARY KEY (`dungeon_id`,`season`,`spec_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_key_throughput definition

CREATE TABLE `aggregated_key_throughput` (
  `season` int NOT NULL,
  `region` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `period_id` int unsigned NOT NULL,
  `run_count` bigint unsigned NOT NULL DEFAULT '0',
  `max_ts` bigint unsigned DEFAULT NULL,
  PRIMARY KEY (`season`,`region`,`period_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_npc_skip_rates definition

CREATE TABLE `aggregated_npc_skip_rates` (
  `dungeon_id` varchar(100) NOT NULL,
  `npc_id` int unsigned NOT NULL,
  `total_encounters` int unsigned NOT NULL DEFAULT '0',
  `total_routes` int unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`dungeon_id`,`npc_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_spec definition

CREATE TABLE `aggregated_spec` (
  `spec_id` int unsigned NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `upgrade_tier` varchar(20) NOT NULL,
  `run_count` bigint unsigned NOT NULL DEFAULT '0',
  `hero_talent_id` int unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`keystone_level`,`spec_id`,`upgrade_tier`,`hero_talent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.bloodlust_spells definition

CREATE TABLE `bloodlust_spells` (
  `spell_id` int unsigned NOT NULL,
  PRIMARY KEY (`spell_id`)
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
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_embellishments definition

CREATE TABLE `global_aggregated_embellishments` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_enchantments_slot_group definition

CREATE TABLE `global_aggregated_enchantments_slot_group` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `slot_group` varchar(100) NOT NULL,
  `enchantment_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`slot_group`,`enchantment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_equipment definition

CREATE TABLE `global_aggregated_equipment` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` varchar(100) NOT NULL,
  `slot` varchar(100) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`,`slot`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_hero_talent_overview definition

CREATE TABLE `global_aggregated_hero_talent_overview` (
  `spec_id` int NOT NULL,
  `hero_talent_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`hero_talent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_item_sockets definition

CREATE TABLE `global_aggregated_item_sockets` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` varchar(100) NOT NULL,
  `socket_item_id` varchar(100) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`,`socket_item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_items definition

CREATE TABLE `global_aggregated_items` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` varchar(100) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`),
  KEY `idx_gai_spec_season` (`spec_id`,`season`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_loadout_data definition

CREATE TABLE `global_aggregated_loadout_data` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `hero_talent_id` int NOT NULL,
  `loadout` varchar(255) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`hero_talent_id`,`loadout`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.global_aggregated_missives definition

CREATE TABLE `global_aggregated_missives` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `item_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.members definition

CREATE TABLE `members` (
  `member` int unsigned NOT NULL AUTO_INCREMENT,
  `spec_id` int NOT NULL,
  `loadout` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `hero_talent_id` int DEFAULT NULL,
  PRIMARY KEY (`member`)
) /*!50100 TABLESPACE `members` */ ENGINE=InnoDB AUTO_INCREMENT=132656940 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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


-- Mythistone.simc_bis_meta definition

CREATE TABLE `simc_bis_meta` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `simc_version` varchar(64) DEFAULT NULL,
  `baseline_dps` double DEFAULT NULL,
  `iterations` int DEFAULT NULL,
  `target_error` double DEFAULT NULL,
  `tier_config` varchar(255) DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`spec_id`,`season`)
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


-- Mythistone.top_player_loadouts definition

CREATE TABLE `top_player_loadouts` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `rank` tinyint unsigned NOT NULL,
  `map_challenge_mode_id` int NOT NULL,
  `region` varchar(32) DEFAULT NULL,
  `character_id` bigint DEFAULT NULL,
  `character_name` varchar(255) DEFAULT NULL,
  `realm` varchar(255) DEFAULT NULL,
  `loadout_key` varchar(255) DEFAULT NULL,
  `loadout_updated_at` datetime DEFAULT NULL,
  `keystone_level` tinyint DEFAULT NULL,
  PRIMARY KEY (`spec_id`,`rank`,`map_challenge_mode_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_class_talent definition

CREATE TABLE `aggregated_class_talent` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `dungeon_id` varchar(100) NOT NULL,
  `hero_talent_id` int NOT NULL,
  `talent_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `avg_rank` double DEFAULT NULL,
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
) /*!50100 TABLESPACE `ts_agregated_equipment` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_hero_talent definition

CREATE TABLE `aggregated_hero_talent` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `dungeon_id` varchar(100) NOT NULL,
  `hero_talent_id` int NOT NULL,
  `talent_id` int NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `avg_rank` double DEFAULT NULL,
  PRIMARY KEY (`spec_id`,`season`,`dungeon_id`,`hero_talent_id`,`talent_id`),
  KEY `dungeon_id` (`dungeon_id`),
  CONSTRAINT `aggregated_hero_talent_ibfk_1` FOREIGN KEY (`dungeon_id`) REFERENCES `dungeon_data` (`dungeon_id`)
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
  `avg_rank` double DEFAULT NULL,
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
) /*!50100 TABLESPACE `ts_character_stats` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
) /*!50100 TABLESPACE `equipments` */ ENGINE=InnoDB AUTO_INCREMENT=396386772 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
) ENGINE=InnoDB AUTO_INCREMENT=411506 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.route_specs definition

CREATE TABLE `route_specs` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `spec_id` int NOT NULL,
  `route_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_route_key` (`route_key`),
  CONSTRAINT `route_specs_route_data_FK` FOREIGN KEY (`route_key`) REFERENCES `route_data` (`route_key`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=136235 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
) /*!50100 TABLESPACE `ts_runs` */ ENGINE=InnoDB AUTO_INCREMENT=65237964 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.simc_bis_items definition

CREATE TABLE `simc_bis_items` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `slot` varchar(32) NOT NULL,
  `rank` tinyint unsigned NOT NULL,
  `item_id` int NOT NULL,
  `bonus_list` varchar(255) DEFAULT NULL,
  `ilevel` int DEFAULT NULL,
  `dps` double DEFAULT NULL,
  `dps_pct_gain` double DEFAULT NULL,
  `is_set_piece` tinyint(1) NOT NULL DEFAULT '0',
  `item_set_id` int DEFAULT NULL,
  PRIMARY KEY (`spec_id`,`season`,`slot`,`rank`),
  KEY `idx_simc_bis_items_spec_season` (`spec_id`,`season`),
  CONSTRAINT `fk_simc_bis_items_meta` FOREIGN KEY (`spec_id`, `season`) REFERENCES `simc_bis_meta` (`spec_id`, `season`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.sockets definition

CREATE TABLE `sockets` (
  `socket_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `socket_item_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `equipment_id` int unsigned NOT NULL,
  `socket_id_pk` bigint unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`socket_id_pk`),
  KEY `sockets_equipment_FK` (`equipment_id`),
  CONSTRAINT `sockets_equipment_FK` FOREIGN KEY (`equipment_id`) REFERENCES `equipment` (`equipment_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=45906020 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.spec_talents definition

CREATE TABLE `spec_talents` (
  `talent_id` int unsigned NOT NULL,
  `member` int unsigned NOT NULL,
  `rank` int NOT NULL,
  PRIMARY KEY (`talent_id`,`member`),
  KEY `spec_talents_run_members_FK` (`member`),
  CONSTRAINT `spec_talents_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) /*!50100 TABLESPACE `spec_talents_vol` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.top_player_loadout_enchants definition

CREATE TABLE `top_player_loadout_enchants` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `rank` tinyint unsigned NOT NULL,
  `map_challenge_mode_id` int NOT NULL,
  `slot_group` varchar(100) NOT NULL,
  `enchantment_id` int NOT NULL,
  PRIMARY KEY (`spec_id`,`rank`,`map_challenge_mode_id`,`slot_group`),
  CONSTRAINT `fk_tpl_enchants_meta` FOREIGN KEY (`spec_id`, `rank`, `map_challenge_mode_id`) REFERENCES `top_player_loadouts` (`spec_id`, `rank`, `map_challenge_mode_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.top_player_loadout_gems definition

CREATE TABLE `top_player_loadout_gems` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `rank` tinyint unsigned NOT NULL,
  `map_challenge_mode_id` int NOT NULL,
  `gem_item_id` int NOT NULL,
  `usage_count` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`rank`,`map_challenge_mode_id`,`gem_item_id`),
  KEY `fk_tpl_gems_meta` (`spec_id`,`rank`,`map_challenge_mode_id`),
  CONSTRAINT `fk_tpl_gems_meta` FOREIGN KEY (`spec_id`, `rank`, `map_challenge_mode_id`) REFERENCES `top_player_loadouts` (`spec_id`, `rank`, `map_challenge_mode_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.top_player_loadout_items definition

CREATE TABLE `top_player_loadout_items` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `rank` tinyint unsigned NOT NULL,
  `map_challenge_mode_id` int NOT NULL,
  `slot` varchar(64) NOT NULL,
  `item_id` int NOT NULL,
  `item_level` smallint DEFAULT NULL,
  PRIMARY KEY (`spec_id`,`rank`,`map_challenge_mode_id`,`slot`),
  CONSTRAINT `fk_tpl_items_meta` FOREIGN KEY (`spec_id`, `rank`, `map_challenge_mode_id`) REFERENCES `top_player_loadouts` (`spec_id`, `rank`, `map_challenge_mode_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.top_player_loadout_talents definition

CREATE TABLE `top_player_loadout_talents` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `rank` tinyint unsigned NOT NULL,
  `map_challenge_mode_id` int NOT NULL,
  `node_id` int NOT NULL,
  `node_rank` tinyint unsigned NOT NULL,
  PRIMARY KEY (`spec_id`,`rank`,`map_challenge_mode_id`,`node_id`),
  CONSTRAINT `fk_tpl_talents_meta` FOREIGN KEY (`spec_id`, `rank`, `map_challenge_mode_id`) REFERENCES `top_player_loadouts` (`spec_id`, `rank`, `map_challenge_mode_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
) ENGINE=InnoDB AUTO_INCREMENT=110060200 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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

CREATE EVENT aggregated_loadout_data_weekly
ON SCHEDULE EVERY 1 DAY
STARTS '2026-02-13 00:00:00.000'
ON COMPLETION NOT PRESERVE
ENABLE
COMMENT 'Daily re-aggregation of loadouts from last 2 weeks'
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- wipe the aggregation table
  TRUNCATE TABLE aggregated_loadout_data;

  -- rebuild from scratch
  INSERT LOW_PRIORITY INTO aggregated_loadout_data
    (spec_id, season, dungeon_id, keystone_level, upgrade_tier, hero_talent_id, loadout, run_count)
  SELECT
    m.spec_id,
    r.season,
    r.dungeon_id,
    r.keystone_level,
    CASE
      WHEN r.duration <= dd.upgrade_1_duration THEN '1'
      WHEN r.duration <= dd.upgrade_2_duration THEN '2'
      WHEN r.duration <= dd.upgrade_3_duration THEN '3'
      ELSE 'depleted'
    END AS upgrade_tier,
    COALESCE(m.hero_talent_id, 0) AS hero_talent_id,
    m.loadout,
    COUNT(DISTINCT r.run_id) AS run_count
  FROM runs r
  JOIN run_members rm ON rm.run_id = r.run_id
  JOIN members m ON m.member = rm.member
  JOIN dungeon_data dd ON dd.dungeon_id = r.dungeon_id
  WHERE r.timestamp > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000  -- Convert to ms for comparison
    AND m.loadout IS NOT NULL
  GROUP BY m.spec_id, r.season, r.dungeon_id, r.keystone_level, upgrade_tier, COALESCE(m.hero_talent_id, 0), m.loadout;
END;

CREATE EVENT aggregated_spec_weekly
ON SCHEDULE EVERY 1 DAY
STARTS '2026-06-06 08:49:38.000'
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Batched aggregation — processes 200K runs at a time to cap temp file size'
DO BEGIN
  DECLARE v_max_season  INT          DEFAULT 0;
  DECLARE v_min_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_max_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_cur         INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size  INT UNSIGNED DEFAULT 200000; -- tune: 200K runs × 5 members = ~1M rows/pass

  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;
  -- Keep each batch's sort in RAM. 1M rows × ~30 bytes = ~30MB comfortably fits.
  SET SESSION sort_buffer_size     = 64 * 1024 * 1024;
  SET SESSION tmp_table_size       = 64 * 1024 * 1024;
  SET SESSION max_heap_table_size  = 64 * 1024 * 1024;

  -- 1. Resolve current season
  SELECT MAX(season) INTO v_max_season FROM Mythistone.runs;

  -- 2. Find the run_id boundaries for this season
  --    (NULL-safe: if no runs exist for the season, the WHILE never executes)
  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE season = v_max_season;

  -- 3. Wipe the target table once, up front
  TRUNCATE TABLE Mythistone.aggregated_spec;

  -- 4. Batch loop — each iteration is a self-contained, small aggregation
  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_spec
      (spec_id, keystone_level, upgrade_tier, run_count, hero_talent_id)
    SELECT
      m.spec_id,
      r.keystone_level,
      CASE
        WHEN r.duration IS NOT NULL AND dd.upgrade_3_duration IS NOT NULL
             AND r.duration <= dd.upgrade_3_duration THEN '3'
        WHEN r.duration IS NOT NULL AND dd.upgrade_2_duration IS NOT NULL
             AND r.duration <= dd.upgrade_2_duration THEN '2'
        WHEN r.duration IS NOT NULL AND dd.upgrade_1_duration IS NOT NULL
             AND r.duration <= dd.upgrade_1_duration THEN '1'
        ELSE 'depleted'
      END                              AS upgrade_tier,
      COUNT(DISTINCT r.run_id)         AS run_count,
      COALESCE(m.hero_talent_id, 0)    AS hero_talent_id
    FROM Mythistone.runs r
    JOIN Mythistone.run_members rm  ON rm.run_id    = r.run_id
    JOIN Mythistone.members m       ON m.member      = rm.member
    JOIN Mythistone.dungeon_data dd ON dd.dungeon_id = r.dungeon_id
    WHERE r.season       = v_max_season
      AND r.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
      AND m.spec_id      IS NOT NULL
      AND r.keystone_level IS NOT NULL
    GROUP BY
      m.spec_id,
      r.keystone_level,
      upgrade_tier,
      COALESCE(m.hero_talent_id, 0)
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count);
      -- ^ accumulates across batches; the PK on (keystone_level, spec_id,
      --   upgrade_tier, hero_talent_id) ensures correct deduplication.

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

END;

CREATE EVENT ev_purge_old_route_data_incremental
ON SCHEDULE EVERY 1 DAY
STARTS '2025-09-27 01:00:00.000'
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Incremental purge of route_data older than 28 days (route_data.timestamp is in seconds)'
DO purge_block: BEGIN
  DECLARE v_cutoff_ts BIGINT DEFAULT 0;        -- seconds
  DECLARE v_route_cutoff BIGINT DEFAULT 0;     -- highest rio_run_id <= cutoff
  DECLARE v_last_ptr BIGINT DEFAULT 0;
  DECLARE v_start BIGINT DEFAULT 0;
  DECLARE v_rio_run_window BIGINT DEFAULT 200000; -- chunk size, tune if needed
  DECLARE v_process_up_to BIGINT DEFAULT 0;

  -- cutoff in seconds (route_data.timestamp stored in seconds)
  SET v_cutoff_ts = UNIX_TIMESTAMP() - 28*24*3600;

  -- determine absolute rio_run_id cutoff (highest rio_run_id whose timestamp <= cutoff)
  SELECT COALESCE(MAX(rio_run_id), 0) INTO v_route_cutoff
  FROM Mythistone.route_data
  WHERE `timestamp` <= v_cutoff_ts;

  -- ensure pointer row exists (separate pointer from the runs pointer)
  INSERT INTO Mythistone.summary_meta (name, last_run_id)
    VALUES ('purge_routes_pointer', 0)
    ON DUPLICATE KEY UPDATE name = name;

  -- LOCK & READ the pointer inside a short transaction
  START TRANSACTION;
    SELECT COALESCE(last_run_id, 0) INTO v_last_ptr
    FROM Mythistone.summary_meta
    WHERE name = 'purge_routes_pointer'
    FOR UPDATE;
  COMMIT;

  SET v_start = v_last_ptr + 1;

  -- nothing to do if no new rio_run_id reached the cutoff yet
  IF v_route_cutoff < v_start THEN
    LEAVE purge_block;
  END IF;

  -- limit the amount processed this invocation
  SET v_process_up_to = LEAST(v_route_cutoff, v_start + v_rio_run_window - 1);

  -- delete route_data rows in this rio_run_id chunk that are older than cutoff seconds
  DELETE rd
  FROM Mythistone.route_data rd
  WHERE rd.rio_run_id BETWEEN v_start AND v_process_up_to
    AND rd.`timestamp` <= v_cutoff_ts;

  -- advance pointer so next run starts after this chunk
  UPDATE Mythistone.summary_meta
  SET last_run_id = v_process_up_to
  WHERE name = 'purge_routes_pointer';
END purge_block;

CREATE EVENT ev_purge_old_run_details_incremental
ON SCHEDULE EVERY 10 MINUTE
STARTS '2026-04-08 20:23:40.000'
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Hyper-fast moving pointer — concurrency-safe via GET_LOCK'
DO purge_block: BEGIN
  DECLARE v_cutoff_ts    BIGINT DEFAULT 0;
  DECLARE v_ptr          BIGINT DEFAULT 0;
  DECLARE v_process_up_to BIGINT DEFAULT 0;
  DECLARE v_member_window BIGINT DEFAULT 40000;
  DECLARE v_found        INT DEFAULT 0;
  DECLARE v_max_member   BIGINT DEFAULT 0;

  -- ── CONCURRENCY GUARD ──────────────────────────────────────────────────
  -- GET_LOCK returns 1 if acquired, 0 if another instance holds it.
  -- Timeout 0 = return immediately rather than queuing.
  IF GET_LOCK('purge_members_lock', 0) = 0 THEN
    LEAVE purge_block;   -- a prior invocation is still running; skip this cycle
  END IF;

  -- 1. Compute cutoff (14 days ago in ms)
  SET v_cutoff_ts = (UNIX_TIMESTAMP() * 1000) - 14 * 24 * 3600 * 1000;

  -- 2. Read/init pointer
  INSERT INTO Mythistone.summary_meta (name, last_run_id)
    VALUES ('purge_member_pointer', 0)
    ON DUPLICATE KEY UPDATE name = name;

  SELECT COALESCE(last_run_id, 0) INTO v_ptr
  FROM Mythistone.summary_meta
  WHERE name = 'purge_member_pointer';

  -- 3. Define bounds
  SELECT COALESCE(MAX(member), 0) INTO v_max_member
  FROM Mythistone.members;

  IF v_ptr >= v_max_member THEN
    SET v_ptr = 0;
  END IF;

  SELECT COALESCE(MIN(member), v_ptr + 1) INTO @min_member
  FROM Mythistone.members
  WHERE member > v_ptr;

  IF @min_member > v_ptr + 1 THEN
    SET v_ptr = @min_member - 1;
  END IF;

  SET v_process_up_to = v_ptr + v_member_window;

  -- 4. MEMORY temp table — 40K INT rows, stays in RAM, zero ibtmp1 cost
  DROP TEMPORARY TABLE IF EXISTS tmp_purge_members;
  CREATE TEMPORARY TABLE tmp_purge_members (
    member INT UNSIGNED PRIMARY KEY
  ) ENGINE=MEMORY;                        -- ← changed from InnoDB

  -- 5. Find purgeable members in window
  INSERT INTO tmp_purge_members (member)
  SELECT rm.member
  FROM Mythistone.run_members rm
  JOIN Mythistone.runs r ON rm.run_id = r.run_id
  WHERE rm.member BETWEEN (v_ptr + 1) AND v_process_up_to
  GROUP BY rm.member
  HAVING MAX(r.timestamp) <= v_cutoff_ts;

  SELECT COUNT(*) INTO v_found FROM tmp_purge_members;

  -- 6. Delete in a transaction
  START TRANSACTION;

  IF v_found > 0 THEN
    DELETE ct FROM Mythistone.class_talents ct
      INNER JOIN tmp_purge_members tmp ON ct.member = tmp.member;
    DELETE ht FROM Mythistone.hero_talents ht
      INNER JOIN tmp_purge_members tmp ON ht.member = tmp.member;
    DELETE st FROM Mythistone.spec_talents st
      INNER JOIN tmp_purge_members tmp ON st.member = tmp.member;
    DELETE eq FROM Mythistone.equipment eq
      INNER JOIN tmp_purge_members tmp ON eq.member = tmp.member;
    DELETE cs FROM Mythistone.character_stats cs
      INNER JOIN tmp_purge_members tmp ON cs.member = tmp.member;
  END IF;

  UPDATE Mythistone.summary_meta
    SET last_run_id = v_process_up_to
  WHERE name = 'purge_member_pointer';

  COMMIT;

  -- 7. Cleanup
  DROP TEMPORARY TABLE IF EXISTS tmp_purge_members;

  -- Release the advisory lock so the next 10-minute tick can proceed
  DO RELEASE_LOCK('purge_members_lock');

END purge_block;

CREATE EVENT ev_update_bonus_list_usage
ON SCHEDULE EVERY 1 DAY
STARTS '2025-09-04 02:00:00.000'
ON COMPLETION PRESERVE
ENABLE
DO BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE v_day DATE;
  DECLARE start_sec BIGINT;
  DECLARE end_sec BIGINT;

  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;
  -- optional (requires SUPER and affects replication): SET SESSION sql_log_bin = 0;

  -- If you want the aggregated table to contain only the recent 14 days, clear it:
  TRUNCATE TABLE Mythistone.aggregated_bonus_lists;

  SET i = 0;
  WHILE i < 14 DO
    SET v_day = DATE_SUB(CURDATE(), INTERVAL i DAY);

    /* compute numeric bounds in seconds */
    SET start_sec = UNIX_TIMESTAMP(v_day);
    SET end_sec   = UNIX_TIMESTAMP(DATE_ADD(v_day, INTERVAL 1 DAY)) - 1;

    INSERT LOW_PRIORITY INTO Mythistone.aggregated_bonus_lists
      (spec_id, season, item_id, bonus_list, run_count)
    SELECT spec_id, season, item_id, bonus_list, run_count
    FROM (
      SELECT
        occ.spec_id,
        occ.season,
        occ.item_id,
        occ.bonus_list,
        COUNT(*) AS run_count
      FROM (
        /* one row per equipment occurrence for runs in this day that have bonus rows */
        SELECT
          M.spec_id,
          R.season,
          EQ.item_id,
          COALESCE(GROUP_CONCAT(DISTINCT B.bonus_id ORDER BY B.bonus_id ASC SEPARATOR ','), '') AS bonus_list,
          R.run_id,
          EQ.equipment_id
        FROM Mythistone.runs R
        JOIN Mythistone.run_members RM ON R.run_id = RM.run_id
        JOIN Mythistone.members M ON RM.member = M.member
        JOIN Mythistone.equipment EQ ON M.member = EQ.member
        JOIN Mythistone.bonus_ids B ON B.equipment_id = EQ.equipment_id
        /* handle both seconds and milliseconds storage: check both ranges */
        WHERE (R.`timestamp` BETWEEN start_sec AND end_sec)
           OR (R.`timestamp` BETWEEN start_sec * 1000 AND end_sec * 1000)
        GROUP BY R.run_id, EQ.equipment_id
      ) AS occ
      WHERE occ.bonus_list <> ''
      GROUP BY occ.spec_id, occ.season, occ.item_id, occ.bonus_list
    ) AS dt
    ON DUPLICATE KEY UPDATE
      run_count = Mythistone.aggregated_bonus_lists.run_count + dt.run_count;

    COMMIT; -- free undo/tmp for this day's batch
    SET i = i + 1;
  END WHILE;

END;

CREATE EVENT ev_update_character_stats_usage
ON SCHEDULE EVERY 1 DAY
STARTS '2025-09-02 02:30:00.000'
ON COMPLETION NOT PRESERVE
ENABLE
DO BEGIN
  -- reduce locking contention and favour low-priority writes
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- wipe the aggregation table and rebuild from scratch
  TRUNCATE TABLE aggregated_character_stats;

  INSERT LOW_PRIORITY INTO aggregated_character_stats
    (spec_id, season, run_count, stat, avg_percent, avg_raw, min_raw, max_raw)
  SELECT
    M.spec_id,
    R.season,
    COUNT(*) AS run_count,                  -- number of member appearances aggregated
    CS.stat,
    AVG(CS.percent) AS avg_percent,         -- AVG ignores NULLs; will be NULL if all NULL
    ROUND(AVG(CS.raw)) AS avg_raw,          -- round to integer to fit bigint column
    MIN(CS.raw) AS min_raw,
    MAX(CS.raw) AS max_raw
  FROM runs R
    JOIN run_members RM ON R.run_id = RM.run_id
    JOIN members M       ON RM.member = M.member
    JOIN character_stats CS ON M.member = CS.member
  GROUP BY
    M.spec_id, R.season, CS.stat;

END;

CREATE EVENT ev_update_class_talent_usage
ON SCHEDULE EVERY 1 WEEK
STARTS '2026-05-05 14:54:30.000'
ON COMPLETION NOT PRESERVE
ENABLE
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- wipe the aggregation table
  TRUNCATE TABLE aggregated_class_talent;

  -- rebuild from scratch
  INSERT LOW_PRIORITY INTO aggregated_class_talent
    (spec_id, season, dungeon_id, hero_talent_id, talent_id, run_count, avg_rank)
  SELECT
    M.spec_id,
    R.season,
    R.dungeon_id,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    CT.talent_id,
    COUNT(*) AS run_count,
    AVG(CT.rank) AS avg_rank
  FROM runs R
    JOIN dungeon_data DD ON R.dungeon_id = DD.dungeon_id
    JOIN run_members RM   ON R.run_id     = RM.run_id
    JOIN members M        ON RM.member    = M.member
    JOIN class_talents CT  ON M.member     = CT.member
  GROUP BY
    M.spec_id, R.season, R.dungeon_id,
    COALESCE(M.hero_talent_id, 0), CT.talent_id;
END;

CREATE EVENT ev_update_crafted_items
ON SCHEDULE EVERY 1 DAY
STARTS '2026-02-13 23:00:00.000'
ON COMPLETION NOT PRESERVE
ENABLE
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- wipe the aggregation tables
  TRUNCATE TABLE aggregated_crafted_items;

  -- rebuild from scratch using crafted_item_ids
  INSERT LOW_PRIORITY INTO aggregated_crafted_items
    (spec_id, season, dungeon_id, keystone_level, upgrade_tier, hero_talent_id, item_id, run_count)
  SELECT
    M.spec_id,
    R.season,
    R.dungeon_id,
    R.keystone_level,
    CASE
      WHEN R.duration <= DD.upgrade_3_duration THEN '3'
      WHEN R.duration <= DD.upgrade_2_duration THEN '2'
      WHEN R.duration <= DD.upgrade_1_duration THEN '1'
      ELSE 'depleted'
    END AS upgrade_tier,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    E.item_id,
    COUNT(*) AS run_count
  FROM runs R
    JOIN dungeon_data DD ON R.dungeon_id = DD.dungeon_id
    JOIN run_members RM   ON R.run_id     = RM.run_id
    JOIN members M        ON RM.member    = M.member
    JOIN equipment E      ON M.member     = E.member
    JOIN crafted_item_ids CII ON E.item_id = CII.item_id
  GROUP BY
    M.spec_id, R.season, R.dungeon_id, R.keystone_level, upgrade_tier,
    COALESCE(M.hero_talent_id, 0), E.item_id;

END;

CREATE EVENT ev_update_dungeon_analytics
ON SCHEDULE EVERY 1 DAY
STARTS '2026-04-06 21:30:00.000'
ON COMPLETION NOT PRESERVE
ENABLE
COMMENT 'Daily re-aggregation of dungeon skip rates and lust stats'
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- wipe the aggregation tables
  TRUNCATE TABLE aggregated_npc_skip_rates;

  -- Update Skip Rates
  INSERT LOW_PRIORITY INTO aggregated_npc_skip_rates (dungeon_id, npc_id, total_encounters, total_routes)
  SELECT 
      rd.dungeon_id,
      pe.npc_id,
      COUNT(DISTINCT rd.route_key) as total_encounters,
      tr.total_routes
  FROM route_data rd
  JOIN pull_enemies pe ON pe.route_key = rd.route_key
  JOIN (
      SELECT dungeon_id, COUNT(DISTINCT route_key) as total_routes
      FROM route_data
      GROUP BY dungeon_id
  ) tr ON rd.dungeon_id = tr.dungeon_id
  GROUP BY rd.dungeon_id, pe.npc_id, tr.total_routes;

END;

CREATE EVENT ev_update_dungeon_specs
ON SCHEDULE EVERY 1 DAY
STARTS '2026-04-07 21:30:00.000'
ON COMPLETION NOT PRESERVE
ENABLE
DO BEGIN
  -- reduce locking contention and favour low-priority writes
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- Update aggregated_dungeon_specs
  TRUNCATE TABLE aggregated_dungeon_specs;
  INSERT LOW_PRIORITY INTO aggregated_dungeon_specs
    (dungeon_id, season, spec_id, run_count)
  SELECT 
    R.dungeon_id, R.season, M.spec_id, COUNT(*) AS run_count
  FROM runs R
  JOIN run_members RM ON R.run_id = RM.run_id
  JOIN members M ON RM.member = M.member
  WHERE R.keystone_level >= 10
  GROUP BY R.dungeon_id, R.season, M.spec_id;

  -- Update aggregated_dungeon_global_specs
  TRUNCATE TABLE aggregated_dungeon_global_specs;
  INSERT LOW_PRIORITY INTO aggregated_dungeon_global_specs
    (season, spec_id, run_count)
  SELECT 
    season, spec_id, SUM(run_count) AS run_count
  FROM aggregated_dungeon_specs
  GROUP BY season, spec_id;

  TRUNCATE TABLE `Mythistone`.`aggregated_dungeon_specs`;

  INSERT LOW_PRIORITY INTO `Mythistone`.`aggregated_dungeon_specs`
	  (dungeon_id, season, spec_id, run_count, max_keystone_level, timed_runs, depleted_runs)
	SELECT 
	  R.dungeon_id, 
	  R.season, 
	  M.spec_id, 
	  COUNT(*) AS run_count,
	  MAX(R.keystone_level) AS max_keystone_level,
	  SUM(CASE WHEN R.duration <= DD.upgrade_1_duration THEN 1 ELSE 0 END) AS timed_runs,
	  SUM(CASE WHEN R.duration <= DD.upgrade_1_duration THEN 0 ELSE 1 END) AS depleted_runs
	FROM `Mythistone`.`runs` R
	JOIN `Mythistone`.`dungeon_data` DD ON DD.dungeon_id = R.dungeon_id
	JOIN `Mythistone`.`run_members` RM ON R.run_id = RM.run_id
	JOIN `Mythistone`.`members` M ON RM.member = M.member
	WHERE R.keystone_level >= 12
	GROUP BY R.dungeon_id, R.season, M.spec_id;

END;

CREATE EVENT ev_update_embellishment_usage
ON SCHEDULE EVERY 1 DAY
STARTS '2025-08-14 03:30:00.000'
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Aggregate embellishment item usage per spec (no slot_group)'
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- ensure pointer exists
  INSERT INTO Mythistone.summary_meta (name, last_run_id)
  VALUES ('embellishment_usage', 0)
  ON DUPLICATE KEY UPDATE name = name;

  INSERT LOW_PRIORITY INTO Mythistone.aggregated_embellishments
    (spec_id, season, dungeon_id, keystone_level, upgrade_tier, hero_talent_id, item_id, run_count)
  SELECT
    t.spec_id,
    t.season,
    t.dungeon_id,
    t.keystone_level,
    t.upgrade_tier,
    t.hero_talent_id,
    t.item_id,
    COUNT(*) AS run_count
  FROM (
    SELECT
      M.spec_id,
      R.season,
      R.dungeon_id,
      R.keystone_level,
      CASE
        WHEN R.duration IS NOT NULL AND DD.upgrade_3_duration IS NOT NULL AND R.duration <= DD.upgrade_3_duration THEN '3'
        WHEN R.duration IS NOT NULL AND DD.upgrade_2_duration IS NOT NULL AND R.duration <= DD.upgrade_2_duration THEN '2'
        WHEN R.duration IS NOT NULL AND DD.upgrade_1_duration IS NOT NULL AND R.duration <= DD.upgrade_1_duration THEN '1'
        ELSE 'depleted'
      END AS upgrade_tier,
      COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
      EM.item_id AS item_id
    FROM Mythistone.runs R
      JOIN Mythistone.dungeon_data DD   ON R.dungeon_id = DD.dungeon_id
      JOIN Mythistone.run_members RM    ON R.run_id = RM.run_id
      JOIN Mythistone.members M         ON RM.member = M.member
      JOIN Mythistone.equipment EQ      ON M.member = EQ.member
      JOIN Mythistone.bonus_ids B       ON B.equipment_id = EQ.equipment_id
      JOIN Mythistone.embellishments EM ON EM.bonus_id = B.bonus_id
    WHERE R.run_id > (
        SELECT COALESCE(last_run_id, 0)
        FROM Mythistone.summary_meta
        WHERE name = 'embellishment_usage'
      )
    GROUP BY R.run_id, EQ.equipment_id, EM.item_id
  ) t
  GROUP BY
    t.spec_id, t.season, t.dungeon_id, t.keystone_level, t.upgrade_tier,
    t.hero_talent_id, t.item_id
  ON DUPLICATE KEY UPDATE
    run_count = run_count + VALUES(run_count);

  -- advance pointer safely
  UPDATE Mythistone.summary_meta
  SET last_run_id = (
      SELECT COALESCE(MAX(run_id), last_run_id)
      FROM Mythistone.runs
      WHERE run_id > summary_meta.last_run_id
    )
  WHERE name = 'embellishment_usage';
END;

CREATE EVENT ev_update_enchantment_usage_slot_group
ON SCHEDULE EVERY 1 DAY
STARTS '2025-08-12 04:00:00.000'
ON COMPLETION PRESERVE
ENABLE
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- full in-place rebuild (no timestamp checks, no summary_meta)
  TRUNCATE TABLE Mythistone.aggregated_enchantments_slot_group;

  INSERT LOW_PRIORITY INTO Mythistone.aggregated_enchantments_slot_group
    (spec_id, season, dungeon_id, keystone_level, upgrade_tier, hero_talent_id, slot_group, enchantment_id, run_count)
  SELECT
    M.spec_id,
    R.season,
    R.dungeon_id,
    R.keystone_level,
    CASE
      WHEN R.duration <= DD.upgrade_3_duration THEN '3'
      WHEN R.duration <= DD.upgrade_2_duration THEN '2'
      WHEN R.duration <= DD.upgrade_1_duration THEN '1'
      ELSE 'depleted'
    END AS upgrade_tier,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    COALESCE(SGM.slot_group, EQ.slot) AS slot_group,
    E.enchantment_id,
    COUNT(*) AS run_count
  FROM Mythistone.runs R
    JOIN Mythistone.dungeon_data DD ON R.dungeon_id = DD.dungeon_id
    JOIN Mythistone.run_members RM ON R.run_id = RM.run_id
    JOIN Mythistone.members M ON RM.member = M.member
    JOIN Mythistone.equipment EQ ON M.member = EQ.member
    JOIN Mythistone.enchantments E ON E.equipment_id = EQ.equipment_id
    LEFT JOIN Mythistone.slot_group_map SGM ON SGM.slot = EQ.slot
  GROUP BY
    M.spec_id, R.season, R.dungeon_id, R.keystone_level, upgrade_tier,
    COALESCE(M.hero_talent_id,0), COALESCE(SGM.slot_group, EQ.slot), E.enchantment_id;
END;

CREATE EVENT ev_update_equipment_usage
ON SCHEDULE EVERY 1 DAY
STARTS '2025-08-13 04:30:00.000'
ON COMPLETION PRESERVE
ENABLE
DO BEGIN
  -- match your existing event's behavior
  DECLARE done INT DEFAULT FALSE;
    DECLARE current_dungeon VARCHAR(100);
    
    -- Cursor to loop through only the dungeons played in the last 14 days
    DECLARE cur CURSOR FOR 
        SELECT DISTINCT dungeon_id 
        FROM Mythistone.runs 
        WHERE timestamp > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000;
        
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET SESSION LOW_PRIORITY_UPDATES = 1;

    -- Start fresh
    TRUNCATE TABLE Mythistone.aggregated_equipment;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO current_dungeon;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Process one dungeon at a time
        INSERT LOW_PRIORITY INTO Mythistone.aggregated_equipment
          (spec_id, season, dungeon_id, keystone_level, upgrade_tier, hero_talent_id, item_id, slot, run_count)
        SELECT
          M.spec_id,
          R.season,
          R.dungeon_id,
          R.keystone_level,
          CASE
            WHEN R.duration <= DD.upgrade_3_duration THEN '3'
            WHEN R.duration <= DD.upgrade_2_duration THEN '2'
            WHEN R.duration <= DD.upgrade_1_duration THEN '1'
            ELSE 'depleted'
          END AS upgrade_tier,
          COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
          EQ.item_id,
          EQ.slot,
          COUNT(DISTINCT R.run_id) AS run_count
        FROM Mythistone.runs R
          JOIN Mythistone.dungeon_data DD ON R.dungeon_id = DD.dungeon_id
          JOIN Mythistone.run_members RM ON R.run_id = RM.run_id
          JOIN Mythistone.members M ON RM.member = M.member
          JOIN Mythistone.equipment EQ ON M.member = EQ.member
        WHERE R.dungeon_id = current_dungeon 
          AND R.timestamp > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000
          AND EQ.item_id IS NOT NULL
        GROUP BY
          M.spec_id, R.season, R.dungeon_id, R.keystone_level, upgrade_tier,
          COALESCE(M.hero_talent_id,0), EQ.item_id, EQ.slot;

    END LOOP;

    CLOSE cur;
END;

CREATE EVENT ev_update_global_aggregates
ON SCHEDULE EVERY 1 DAY
STARTS '2025-08-16 16:00:00.000'
ON COMPLETION PRESERVE
ENABLE
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- 1. Equipment
  TRUNCATE TABLE Mythistone.global_aggregated_equipment;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_equipment (spec_id, season, item_id, slot, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, item_id, slot, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_equipment
  GROUP BY spec_id, season, item_id, slot;

  -- 2. Enchantments
  TRUNCATE TABLE Mythistone.global_aggregated_enchantments_slot_group;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_enchantments_slot_group (spec_id, season, slot_group, enchantment_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, slot_group, enchantment_id, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_enchantments_slot_group
  GROUP BY spec_id, season, slot_group, enchantment_id;

  -- 3. Sockets
  TRUNCATE TABLE Mythistone.global_aggregated_item_sockets;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_item_sockets (spec_id, season, item_id, socket_item_id, run_count, max_timed_key, max_depleted_key)
  SELECT
    M.spec_id,
    COALESCE(R.season, 0) AS season,
    EQ.item_id,
    s.socket_item_id,
    COUNT(s.socket_id_pk) AS run_count,
    MAX(IF(R.duration <= DD.upgrade_1_duration, R.keystone_level, 0)) AS max_timed_key,
    MAX(IF(R.duration > DD.upgrade_1_duration, R.keystone_level, 0)) AS max_depleted_key
  FROM Mythistone.runs R
    JOIN Mythistone.dungeon_data DD   ON R.dungeon_id = DD.dungeon_id
    JOIN Mythistone.run_members RM    ON R.run_id = RM.run_id
    JOIN Mythistone.members M         ON RM.member = M.member
    JOIN Mythistone.equipment EQ      ON M.member = EQ.member
    JOIN Mythistone.sockets s         ON s.equipment_id = EQ.equipment_id
  GROUP BY M.spec_id, COALESCE(R.season, 0), EQ.item_id, s.socket_item_id;

  -- 4. Missives
  TRUNCATE TABLE Mythistone.global_aggregated_missives;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_missives (spec_id, season, item_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, item_id, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_missives
  GROUP BY spec_id, season, item_id;

  -- 5. Embellishments
  TRUNCATE TABLE Mythistone.global_aggregated_embellishments;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_embellishments (spec_id, season, item_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, item_id, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_embellishments
  GROUP BY spec_id, season, item_id;

  -- 6. Hero Talents
  TRUNCATE TABLE Mythistone.global_aggregated_hero_talent_overview;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_hero_talent_overview (spec_id, hero_talent_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, hero_talent_id, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_spec
  GROUP BY spec_id, hero_talent_id;

  -- 7. Loadouts
  TRUNCATE TABLE Mythistone.global_aggregated_loadout_data;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_loadout_data (spec_id, season, hero_talent_id, loadout, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, hero_talent_id_key AS hero_talent_id, loadout_key AS loadout, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_loadout_data
  WHERE loadout_key != '<NULL>'
  GROUP BY spec_id, season, hero_talent_id_key, loadout_key;
  
  -- 8 Global Equipment without Slot (for true max keys per item)
  TRUNCATE TABLE Mythistone.global_aggregated_items;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_items (spec_id, season, item_id, run_count, max_timed_key, max_depleted_key)
  SELECT 
    spec_id, 
    season, 
    item_id, 
    SUM(run_count),
    MAX(CASE WHEN upgrade_tier != 'depleted' THEN keystone_level ELSE 0 END),
    MAX(CASE WHEN upgrade_tier = 'depleted' THEN keystone_level ELSE 0 END)
  FROM Mythistone.aggregated_equipment
  GROUP BY spec_id, season, item_id;
  
  -- crafted items
  TRUNCATE TABLE Mythistone.global_aggregated_crafted_items;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_crafted_items (spec_id, season, item_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, item_id, SUM(run_count) AS run_count,
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_crafted_items
  GROUP BY spec_id, season, item_id;

END;

CREATE EVENT ev_update_global_aggregates_bonus_lists
ON SCHEDULE EVERY 1 DAY
STARTS '2025-08-16 07:00:00.000'
ON COMPLETION PRESERVE
ENABLE
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  TRUNCATE TABLE Mythistone.global_aggregated_bonus_lists;
  INSERT LOW_PRIORITY INTO Mythistone.global_aggregated_bonus_lists (spec_id, season, item_id, bonus_list, run_count)
  SELECT spec_id, season, item_id, bonus_list, SUM(run_count)
  FROM Mythistone.aggregated_bonus_lists
  GROUP BY spec_id, season, item_id, bonus_list;
END;

CREATE EVENT ev_update_hero_talent_usage
ON SCHEDULE EVERY 1 DAY
STARTS '2025-08-31 05:00:00.000'
ON COMPLETION NOT PRESERVE
ENABLE
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- wipe the aggregation table
  TRUNCATE TABLE aggregated_hero_talent;

  -- rebuild from scratch
  INSERT LOW_PRIORITY INTO aggregated_hero_talent
    (spec_id, season, dungeon_id, hero_talent_id, talent_id, run_count, avg_rank)
  SELECT
    M.spec_id,
    R.season,
    R.dungeon_id,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    HT.talent_id,
    COUNT(*) AS run_count,
    AVG(HT.rank) AS avg_rank
  FROM runs R
    JOIN dungeon_data DD ON R.dungeon_id = DD.dungeon_id
    JOIN run_members RM   ON R.run_id     = RM.run_id
    JOIN members M        ON RM.member    = M.member
    JOIN hero_talents HT  ON M.member     = HT.member
  GROUP BY
    M.spec_id, R.season, R.dungeon_id,
    COALESCE(M.hero_talent_id, 0), HT.talent_id;

END;

CREATE EVENT ev_update_key_throughput
ON SCHEDULE EVERY 1 DAY
STARTS '2026-06-22 20:17:50.000'
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Batched aggregation — processes 200K runs at a time to cap the season_periods range-join cost for the dashboard Keys/Minute card'
DO BEGIN
  DECLARE v_max_season  INT          DEFAULT 0;
  DECLARE v_min_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_max_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_cur         INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size  INT UNSIGNED DEFAULT 200000; -- run_ids per pass

  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;
  -- Keep each batch's grouping in RAM rather than spilling to temp files on disk.
  SET SESSION sort_buffer_size     = 64 * 1024 * 1024;
  SET SESSION tmp_table_size       = 64 * 1024 * 1024;
  SET SESSION max_heap_table_size  = 64 * 1024 * 1024;

  -- 1. Resolve current season
  SELECT MAX(season) INTO v_max_season FROM Mythistone.runs;

  -- 2. Find the run_id boundaries for this season
  --    (NULL-safe: if no runs exist for the season, the WHILE never executes)
  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE season = v_max_season;

  -- 3. Wipe the target table once, up front
  TRUNCATE TABLE Mythistone.aggregated_key_throughput;

  -- 4. Batch loop — each pass aggregates one run_id window and merges it in
  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_key_throughput
      (season, region, period_id, run_count, max_ts)
    SELECT
      R.season,
      R.region,
      SP.period_id,
      COUNT(*)         AS run_count,
      MAX(R.timestamp) AS max_ts
    FROM Mythistone.runs R
    JOIN Mythistone.season_periods SP
      ON SP.region = R.region
     AND SP.season = R.season
     AND R.timestamp >= SP.start_timestamp
     AND R.timestamp <  SP.end_timestamp
    WHERE R.season = v_max_season
      AND R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
    GROUP BY R.season, R.region, SP.period_id
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count),
      max_ts    = GREATEST(max_ts, VALUES(max_ts));
      -- ^ accumulates across batches; the PK on (season, region, period_id)
      --   merges passes that touch the same period.

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

END;

CREATE EVENT ev_update_missive_usage
ON SCHEDULE EVERY 1 DAY
STARTS '2025-08-14 05:30:00.000'
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Aggregate missive item usage per spec (no slot_group)'
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- ensure pointer exists
  INSERT INTO Mythistone.summary_meta (name, last_run_id)
  VALUES ('missive_usage', 0)
  ON DUPLICATE KEY UPDATE name = name;

  -- inner -> one row per run+equipment+missive_item
  INSERT LOW_PRIORITY INTO Mythistone.aggregated_missives
    (spec_id, season, dungeon_id, keystone_level, upgrade_tier, hero_talent_id, item_id, run_count)
  SELECT
    t.spec_id,
    t.season,
    t.dungeon_id,
    t.keystone_level,
    t.upgrade_tier,
    t.hero_talent_id,
    t.item_id,
    COUNT(*) AS run_count
  FROM (
    SELECT
      M.spec_id,
      R.season,
      R.dungeon_id,
      R.keystone_level,
      CASE
        WHEN R.duration IS NOT NULL AND DD.upgrade_3_duration IS NOT NULL AND R.duration <= DD.upgrade_3_duration THEN '3'
        WHEN R.duration IS NOT NULL AND DD.upgrade_2_duration IS NOT NULL AND R.duration <= DD.upgrade_2_duration THEN '2'
        WHEN R.duration IS NOT NULL AND DD.upgrade_1_duration IS NOT NULL AND R.duration <= DD.upgrade_1_duration THEN '1'
        ELSE 'depleted'
      END AS upgrade_tier,
      COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
      MS.item_id AS item_id
    FROM Mythistone.runs R
      JOIN Mythistone.dungeon_data DD   ON R.dungeon_id = DD.dungeon_id
      JOIN Mythistone.run_members RM    ON R.run_id = RM.run_id
      JOIN Mythistone.members M         ON RM.member = M.member
      JOIN Mythistone.equipment EQ      ON M.member = EQ.member
      JOIN Mythistone.bonus_ids B       ON B.equipment_id = EQ.equipment_id
      JOIN Mythistone.missives MS       ON MS.bonus_id = B.bonus_id
    WHERE R.run_id > (
        SELECT COALESCE(last_run_id, 0)
        FROM Mythistone.summary_meta
        WHERE name = 'missive_usage'
      )
    GROUP BY R.run_id, EQ.equipment_id, MS.item_id
  ) t
  GROUP BY
    t.spec_id, t.season, t.dungeon_id, t.keystone_level, t.upgrade_tier,
    t.hero_talent_id, t.item_id
  ON DUPLICATE KEY UPDATE
    run_count = run_count + VALUES(run_count);

  -- advance pointer safely
  UPDATE Mythistone.summary_meta
  SET last_run_id = (
      SELECT COALESCE(MAX(run_id), last_run_id)
      FROM Mythistone.runs
      WHERE run_id > summary_meta.last_run_id
    )
  WHERE name = 'missive_usage';
END;

CREATE EVENT ev_update_spec_talent_usage
ON SCHEDULE EVERY 1 DAY
STARTS '2026-05-07 22:00:00.000'
ON COMPLETION NOT PRESERVE
ENABLE
DO BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  -- wipe the aggregation table
  TRUNCATE TABLE aggregated_spec_talent;

  -- rebuild from scratch
  INSERT LOW_PRIORITY INTO aggregated_spec_talent
    (spec_id, season, dungeon_id, hero_talent_id, talent_id, run_count, avg_rank)
  SELECT
    M.spec_id,
    R.season,
    R.dungeon_id,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    ST.talent_id,
    COUNT(*) AS run_count,
    AVG(ST.rank) AS avg_rank
  FROM runs R
    JOIN run_members RM   ON R.run_id     = RM.run_id
    JOIN members M        ON RM.member    = M.member
    JOIN spec_talents ST  ON M.member     = ST.member
  GROUP BY
    M.spec_id, R.season, R.dungeon_id,
    COALESCE(M.hero_talent_id, 0), ST.talent_id;

END;