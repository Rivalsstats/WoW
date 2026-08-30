-- Mythistone.agg_lock_diag definition

CREATE TABLE `agg_lock_diag` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `captured_at` datetime NOT NULL,
  `step` varchar(100) DEFAULT NULL,
  `target_table` varchar(128) DEFAULT NULL,
  `holder_processlist_id` bigint DEFAULT NULL,
  `holder_user` varchar(128) DEFAULT NULL,
  `holder_host` varchar(255) DEFAULT NULL,
  `holder_command` varchar(64) DEFAULT NULL,
  `holder_time` bigint DEFAULT NULL,
  `holder_state` varchar(128) DEFAULT NULL,
  `holder_info` text,
  `lock_type` varchar(64) DEFAULT NULL,
  `lock_status` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_agg_lock_diag_captured` (`captured_at`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.agg_pipeline_log definition

CREATE TABLE `agg_pipeline_log` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `step` varchar(100) NOT NULL,
  `started_at` datetime NOT NULL,
  `finished_at` datetime DEFAULT NULL,
  `error` text,
  PRIMARY KEY (`id`),
  KEY `idx_agg_pipeline_log_step` (`step`,`started_at`)
) ENGINE=InnoDB AUTO_INCREMENT=983 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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


-- Mythistone.aggregated_bonus_lists_new definition

CREATE TABLE `aggregated_bonus_lists_new` (
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
  KEY `dungeon_id` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_completion_heatmap definition

CREATE TABLE `aggregated_completion_heatmap` (
  `season` int NOT NULL,
  `region` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `day_of_week` tinyint unsigned NOT NULL,
  `hour_of_day` tinyint unsigned NOT NULL,
  `run_count` bigint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`season`,`region`,`day_of_week`,`hour_of_day`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_crafted_comps definition

CREATE TABLE `aggregated_crafted_comps` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `comp` varchar(255) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`hero_talent_id`,`comp`)
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
  KEY `aggregated_crafted_items_fk_dd` (`dungeon_id`)
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


-- Mythistone.aggregated_embellishment_comps definition

CREATE TABLE `aggregated_embellishment_comps` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `comp` varchar(255) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`hero_talent_id`,`comp`)
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
  KEY `aggregated_embellishments_fk_dd` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_enchant_comps definition

CREATE TABLE `aggregated_enchant_comps` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `comp` varchar(255) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`hero_talent_id`,`comp`)
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
  KEY `enchantment_id_idx` (`enchantment_id`)
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
  KEY `item_id_idx` (`item_id`)
) /*!50100 TABLESPACE `ts_agregated_equipment` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_equipment_new definition

CREATE TABLE `aggregated_equipment_new` (
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
  KEY `item_id_idx` (`item_id`)
) /*!50100 TABLESPACE `ts_agregated_equipment` */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_gem_comps definition

CREATE TABLE `aggregated_gem_comps` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `comp` varchar(255) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`hero_talent_id`,`comp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
  KEY `dungeon_id` (`dungeon_id`)
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
  KEY `idx_spec` (`spec_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_loadout_data_new definition

CREATE TABLE `aggregated_loadout_data_new` (
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
  KEY `idx_spec` (`spec_id`)
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
  KEY `aggregated_missives_fk_dd` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_missives_new definition

CREATE TABLE `aggregated_missives_new` (
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
  KEY `aggregated_missives_fk_dd` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_npc_skip_rates definition

CREATE TABLE `aggregated_npc_skip_rates` (
  `dungeon_id` varchar(100) NOT NULL,
  `npc_id` int unsigned NOT NULL,
  `total_encounters` int unsigned NOT NULL DEFAULT '0',
  `total_routes` int unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`dungeon_id`,`npc_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_runs_per_dungeon_per_level definition

CREATE TABLE `aggregated_runs_per_dungeon_per_level` (
  `season` int NOT NULL,
  `dungeon_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `keystone_level` int unsigned NOT NULL,
  `tier_3` bigint unsigned NOT NULL DEFAULT '0',
  `tier_2` bigint unsigned NOT NULL DEFAULT '0',
  `tier_1` bigint unsigned NOT NULL DEFAULT '0',
  `depleted` bigint unsigned NOT NULL DEFAULT '0',
  `total_runs` bigint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`season`,`dungeon_id`,`keystone_level`)
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
  KEY `dungeon_id` (`dungeon_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.aggregated_tier_set_comps definition

CREATE TABLE `aggregated_tier_set_comps` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL DEFAULT '0',
  `hero_talent_id` int NOT NULL DEFAULT '0',
  `comp` varchar(255) NOT NULL,
  `run_count` bigint NOT NULL DEFAULT '0',
  `max_timed_key` tinyint unsigned NOT NULL DEFAULT '0',
  `max_depleted_key` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`spec_id`,`season`,`hero_talent_id`,`comp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.bloodlust_spells definition

CREATE TABLE `bloodlust_spells` (
  `spell_id` int unsigned NOT NULL,
  PRIMARY KEY (`spell_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.bonus_migration_log definition

CREATE TABLE `bonus_migration_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `ts` datetime NOT NULL,
  `phase` varchar(20) NOT NULL,
  `detail` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.bonus_sets definition

CREATE TABLE `bonus_sets` (
  `set_id` binary(16) NOT NULL,
  `bonus_id` int unsigned NOT NULL,
  PRIMARY KEY (`set_id`,`bonus_id`)
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
  `talent_set_id` binary(16) DEFAULT NULL,
  PRIMARY KEY (`member`),
  KEY `members_talent_set_id_IDX` (`talent_set_id`)
) /*!50100 TABLESPACE `members` */ ENGINE=InnoDB AUTO_INCREMENT=7168924 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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


-- Mythistone.simc_bis_progress_meta definition

CREATE TABLE `simc_bis_progress_meta` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `run_signature` char(64) NOT NULL,
  `total_profilesets` int NOT NULL,
  `baseline_dps` double DEFAULT NULL,
  `simc_version` varchar(64) DEFAULT NULL,
  `started_at` datetime DEFAULT NULL,
  `last_attempt_at` datetime DEFAULT NULL,
  `failed` tinyint(1) NOT NULL DEFAULT '0',
  `prep_snapshot` mediumtext,
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


-- Mythistone.talent_sets definition

CREATE TABLE `talent_sets` (
  `set_id` binary(16) NOT NULL,
  `tree` tinyint NOT NULL,
  `talent_id` int unsigned NOT NULL,
  `rank` int NOT NULL,
  PRIMARY KEY (`set_id`,`tree`,`talent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.tier_set_items definition

CREATE TABLE `tier_set_items` (
  `item_id` int NOT NULL,
  `item_set_id` int NOT NULL,
  PRIMARY KEY (`item_id`),
  KEY `idx_tier_set_items_set` (`item_set_id`)
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


-- Mythistone.trend_snapshot definition

CREATE TABLE `trend_snapshot` (
  `week_id` int unsigned NOT NULL,
  `feed` varchar(24) NOT NULL,
  `group_key` varchar(100) NOT NULL DEFAULT '',
  `entity_key` varchar(128) NOT NULL,
  `label` varchar(255) DEFAULT NULL,
  `tier` tinyint DEFAULT NULL,
  `rank_pos` smallint DEFAULT NULL,
  `score` double DEFAULT NULL,
  `popularity` double NOT NULL DEFAULT '0',
  `run_count` bigint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`week_id`,`feed`,`group_key`,`entity_key`),
  KEY `idx_trend_feed_group_week` (`feed`,`group_key`,`week_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.wipe_control definition

CREATE TABLE `wipe_control` (
  `id` tinyint NOT NULL DEFAULT '1',
  `request_season` int NOT NULL DEFAULT '0',
  `done_season` int NOT NULL DEFAULT '0',
  `collector_paused` tinyint NOT NULL DEFAULT '0',
  `collector_beat` bigint NOT NULL DEFAULT '0',
  `requested_at` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  CONSTRAINT `chk_wipe_control_single_row` CHECK ((`id` = 1))
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


-- Mythistone.equipment definition

CREATE TABLE `equipment` (
  `slot` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `item_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `item_level` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `member` int unsigned NOT NULL,
  `equipment_id` int unsigned NOT NULL AUTO_INCREMENT,
  `bonus_set_id` binary(16) DEFAULT NULL,
  PRIMARY KEY (`equipment_id`),
  KEY `equipment_run_members_FK` (`member`),
  KEY `equipment_bonus_set_id_IDX` (`bonus_set_id`),
  CONSTRAINT `equipment_run_members_FK` FOREIGN KEY (`member`) REFERENCES `members` (`member`) ON DELETE CASCADE ON UPDATE CASCADE
) /*!50100 TABLESPACE `equipments` */ ENGINE=InnoDB AUTO_INCREMENT=79282698 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.route_pulls definition

CREATE TABLE `route_pulls` (
  `route_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `pull_id` int unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`pull_id`,`route_key`),
  KEY `route_pulls_route_data_FK` (`route_key`),
  CONSTRAINT `route_pulls_route_data_FK` FOREIGN KEY (`route_key`) REFERENCES `route_data` (`route_key`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=40124 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.route_specs definition

CREATE TABLE `route_specs` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `spec_id` int NOT NULL,
  `route_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_route_key` (`route_key`),
  CONSTRAINT `route_specs_route_data_FK` FOREIGN KEY (`route_key`) REFERENCES `route_data` (`route_key`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=13301 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
) /*!50100 TABLESPACE `ts_runs` */ ENGINE=InnoDB AUTO_INCREMENT=1581051 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
  `enchant_id` int DEFAULT NULL,
  `gem_ids` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`spec_id`,`season`,`slot`,`rank`),
  KEY `idx_simc_bis_items_spec_season` (`spec_id`,`season`),
  CONSTRAINT `fk_simc_bis_items_meta` FOREIGN KEY (`spec_id`, `season`) REFERENCES `simc_bis_meta` (`spec_id`, `season`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Mythistone.simc_bis_progress definition

CREATE TABLE `simc_bis_progress` (
  `spec_id` int NOT NULL,
  `season` int NOT NULL,
  `profileset_name` varchar(32) NOT NULL,
  `mean_dps` double NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`spec_id`,`season`,`profileset_name`),
  KEY `idx_simc_bis_progress_spec_season` (`spec_id`,`season`),
  CONSTRAINT `fk_simc_bis_progress_meta` FOREIGN KEY (`spec_id`, `season`) REFERENCES `simc_bis_progress_meta` (`spec_id`, `season`) ON DELETE CASCADE
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
) ENGINE=InnoDB AUTO_INCREMENT=16248344 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
  `bonus_ids` varchar(255) DEFAULT NULL,
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
  `entry_id` int DEFAULT NULL,
  `spell_id` int DEFAULT NULL,
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
) ENGINE=InnoDB AUTO_INCREMENT=35100645 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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


CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_bonus_lists`()
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE v_day DATE;
  DECLARE start_sec BIGINT;
  DECLARE end_sec BIGINT;

  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.aggregated_bonus_lists_new, Mythistone.aggregated_bonus_lists_old;
  CREATE TABLE Mythistone.aggregated_bonus_lists_new LIKE Mythistone.aggregated_bonus_lists;

  SET i = 0;
  WHILE i < 14 DO
    SET v_day = DATE_SUB(CURDATE(), INTERVAL i DAY);

    /* compute numeric bounds in seconds */
    SET start_sec = UNIX_TIMESTAMP(v_day);
    SET end_sec   = UNIX_TIMESTAMP(DATE_ADD(v_day, INTERVAL 1 DAY)) - 1;

    INSERT LOW_PRIORITY INTO Mythistone.aggregated_bonus_lists_new
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
        JOIN Mythistone.bonus_sets B ON B.set_id = EQ.bonus_set_id
        /* handle both seconds and milliseconds storage: check both ranges */
        WHERE (R.`timestamp` BETWEEN start_sec AND end_sec)
           OR (R.`timestamp` BETWEEN start_sec * 1000 AND end_sec * 1000)
        GROUP BY R.run_id, EQ.equipment_id
      ) AS occ
      WHERE occ.bonus_list <> ''
      GROUP BY occ.spec_id, occ.season, occ.item_id, occ.bonus_list
    ) AS dt
    ON DUPLICATE KEY UPDATE
      run_count = Mythistone.aggregated_bonus_lists_new.run_count + dt.run_count;

    SET i = i + 1;
  END WHILE;

  RENAME TABLE Mythistone.aggregated_bonus_lists     TO Mythistone.aggregated_bonus_lists_old,
               Mythistone.aggregated_bonus_lists_new TO Mythistone.aggregated_bonus_lists;
  DROP TABLE Mythistone.aggregated_bonus_lists_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_bonus_sets_gc`()
BEGIN
  -- Orphan sweep for the bonus dictionary. equipment.bonus_set_id has no FK to
  -- bonus_sets (an FK would block the season-wipe TRUNCATE), so equipment rows
  -- that get purged or wiped leave their dictionary rows behind. Each aggregation
  -- cycle deletes bonus_sets rows whose set_id is no longer referenced by any
  -- equipment. The anti-join uses the index on equipment.bonus_set_id. This is
  -- not a shadow swap; it runs through sp_run_agg_step only for its retry/logging
  -- wrapper.
  CALL sp_agg_session_setup();

  DELETE BS FROM Mythistone.bonus_sets BS
  WHERE NOT EXISTS (
    SELECT 1 FROM Mythistone.equipment EQ
    WHERE EQ.bonus_set_id = BS.set_id
  );
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_character_stats`()
BEGIN
  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.aggregated_character_stats_new, Mythistone.aggregated_character_stats_old;
  CREATE TABLE Mythistone.aggregated_character_stats_new LIKE Mythistone.aggregated_character_stats;

  INSERT INTO Mythistone.aggregated_character_stats_new
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
  FROM Mythistone.runs R
    JOIN Mythistone.run_members RM       ON R.run_id = RM.run_id
    JOIN Mythistone.members M            ON RM.member = M.member
    JOIN Mythistone.character_stats CS   ON M.member = CS.member
  WHERE R.`timestamp` > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000
  GROUP BY
    M.spec_id, R.season, CS.stat;

  RENAME TABLE Mythistone.aggregated_character_stats     TO Mythistone.aggregated_character_stats_old,
               Mythistone.aggregated_character_stats_new TO Mythistone.aggregated_character_stats;
  DROP TABLE Mythistone.aggregated_character_stats_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_class_talent`()
BEGIN
  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.aggregated_class_talent_new, Mythistone.aggregated_class_talent_old;
  CREATE TABLE Mythistone.aggregated_class_talent_new LIKE Mythistone.aggregated_class_talent;

  INSERT INTO Mythistone.aggregated_class_talent_new
    (spec_id, season, dungeon_id, hero_talent_id, talent_id, run_count, avg_rank)
  SELECT
    M.spec_id,
    R.season,
    R.dungeon_id,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    CT.talent_id,
    COUNT(*) AS run_count,
    AVG(CT.rank) AS avg_rank
  FROM Mythistone.runs R
    JOIN Mythistone.dungeon_data DD   ON R.dungeon_id = DD.dungeon_id
    JOIN Mythistone.run_members RM    ON R.run_id     = RM.run_id
    JOIN Mythistone.members M         ON RM.member    = M.member
    JOIN Mythistone.talent_sets CT    ON CT.set_id    = M.talent_set_id AND CT.tree = 0
  WHERE R.`timestamp` > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000
  GROUP BY
    M.spec_id, R.season, R.dungeon_id,
    COALESCE(M.hero_talent_id, 0), CT.talent_id;

  RENAME TABLE Mythistone.aggregated_class_talent     TO Mythistone.aggregated_class_talent_old,
               Mythistone.aggregated_class_talent_new TO Mythistone.aggregated_class_talent;
  DROP TABLE Mythistone.aggregated_class_talent_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_completion_heatmap`()
BEGIN
  DECLARE v_min_run    INT UNSIGNED DEFAULT 1;
  DECLARE v_max_run    INT UNSIGNED DEFAULT 0;
  DECLARE v_cur        INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size INT UNSIGNED DEFAULT 1000000; -- runs only, no joins

  CALL sp_agg_session_setup();

  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs;

  DROP TABLE IF EXISTS Mythistone.aggregated_completion_heatmap_new, Mythistone.aggregated_completion_heatmap_old;
  CREATE TABLE Mythistone.aggregated_completion_heatmap_new LIKE Mythistone.aggregated_completion_heatmap;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_completion_heatmap_new
      (season, region, day_of_week, hour_of_day, run_count)
    SELECT
      R.season,
      R.region,
      MOD(FLOOR(IF(R.`timestamp` > 100000000000, R.`timestamp` DIV 1000, R.`timestamp`) / 86400) + 4, 7) AS day_of_week,
      MOD(FLOOR(IF(R.`timestamp` > 100000000000, R.`timestamp` DIV 1000, R.`timestamp`) / 3600), 24)     AS hour_of_day,
      COUNT(*) AS run_count
    FROM Mythistone.runs R
    WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
    GROUP BY R.season, R.region, day_of_week, hour_of_day
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count);

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE Mythistone.aggregated_completion_heatmap     TO Mythistone.aggregated_completion_heatmap_old,
               Mythistone.aggregated_completion_heatmap_new TO Mythistone.aggregated_completion_heatmap;
  DROP TABLE Mythistone.aggregated_completion_heatmap_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_crafted_items`()
BEGIN
  DECLARE v_cutoff_ms  BIGINT       DEFAULT 0;
  DECLARE v_min_run    INT UNSIGNED DEFAULT 1;
  DECLARE v_max_run    INT UNSIGNED DEFAULT 0;
  DECLARE v_cur        INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size INT UNSIGNED DEFAULT 200000;

  CALL sp_agg_session_setup();

  SET v_cutoff_ms = UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000;

  SELECT COALESCE(MIN(run_id), 1), COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE `timestamp` > v_cutoff_ms;

  DROP TABLE IF EXISTS Mythistone.aggregated_crafted_items_new, Mythistone.aggregated_crafted_items_old;
  CREATE TABLE Mythistone.aggregated_crafted_items_new LIKE Mythistone.aggregated_crafted_items;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_crafted_items_new
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
    FROM Mythistone.runs R
      JOIN Mythistone.dungeon_data DD ON R.dungeon_id = DD.dungeon_id
      JOIN Mythistone.run_members RM   ON R.run_id     = RM.run_id
      JOIN Mythistone.members M        ON RM.member    = M.member
      JOIN Mythistone.equipment E      ON M.member     = E.member
      JOIN Mythistone.crafted_item_ids CII ON E.item_id = CII.item_id
    WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
      AND R.`timestamp` > v_cutoff_ms
    GROUP BY
      M.spec_id, R.season, R.dungeon_id, R.keystone_level, upgrade_tier,
      COALESCE(M.hero_talent_id, 0), E.item_id
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count);

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE Mythistone.aggregated_crafted_items     TO Mythistone.aggregated_crafted_items_old,
               Mythistone.aggregated_crafted_items_new TO Mythistone.aggregated_crafted_items;
  DROP TABLE Mythistone.aggregated_crafted_items_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_dungeon_analytics`()
BEGIN
  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.aggregated_npc_skip_rates_new, Mythistone.aggregated_npc_skip_rates_old;
  CREATE TABLE Mythistone.aggregated_npc_skip_rates_new LIKE Mythistone.aggregated_npc_skip_rates;

  INSERT INTO Mythistone.aggregated_npc_skip_rates_new (dungeon_id, npc_id, total_encounters, total_routes)
  SELECT
      rd.dungeon_id,
      pe.npc_id,
      COUNT(DISTINCT rd.route_key) as total_encounters,
      tr.total_routes
  FROM Mythistone.route_data rd
  JOIN Mythistone.pull_enemies pe ON pe.route_key = rd.route_key
  JOIN (
      SELECT dungeon_id, COUNT(DISTINCT route_key) as total_routes
      FROM Mythistone.route_data
      GROUP BY dungeon_id
  ) tr ON rd.dungeon_id = tr.dungeon_id
  GROUP BY rd.dungeon_id, pe.npc_id, tr.total_routes;

  RENAME TABLE Mythistone.aggregated_npc_skip_rates     TO Mythistone.aggregated_npc_skip_rates_old,
               Mythistone.aggregated_npc_skip_rates_new TO Mythistone.aggregated_npc_skip_rates;
  DROP TABLE Mythistone.aggregated_npc_skip_rates_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_dungeon_comps`()
BEGIN
  DECLARE v_max_season  INT          DEFAULT 0;
  DECLARE v_min_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_max_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_cur         INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size  INT UNSIGNED DEFAULT 200000; -- tune: 200K runs × 5 members = ~1M rows/pass
  DECLARE v_min_level   INT UNSIGNED DEFAULT 10;     -- matches sp_agg_dungeon_specs' global pass

  CALL sp_agg_session_setup();

  -- Full rebuild of the current season into a shadow table, then atomic swap.
  -- Batching on run_id is safe because a comp group is scoped to a single run
  -- and can never span a batch boundary; the ON DUPLICATE KEY UPDATE
  -- accumulates the per-batch partial aggregates.

  -- 1. Resolve current season
  SELECT MAX(season) INTO v_max_season FROM Mythistone.runs;

  -- 2. Find the run_id boundaries for this season
  --    (NULL-safe: if no runs exist for the season, the WHILE never executes)
  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE season = v_max_season;

  DROP TABLE IF EXISTS Mythistone.aggregated_dungeon_comps_new, Mythistone.aggregated_dungeon_comps_old;
  CREATE TABLE Mythistone.aggregated_dungeon_comps_new LIKE Mythistone.aggregated_dungeon_comps;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT LOW_PRIORITY INTO Mythistone.aggregated_dungeon_comps_new
      (dungeon_id, season, keystone_level, comp, timed_runs, depleted_runs)
    SELECT
      c.dungeon_id,
      c.season,
      c.keystone_level,
      c.comp,
      SUM(c.timed)     AS timed_runs,
      SUM(1 - c.timed) AS depleted_runs
    FROM (
      -- one row per run: the canonical comp string plus the run's outcome.
      -- Ordering the spec_ids ascending is what makes `comp` canonical -- the
      -- same five specs must always produce the same string, or GROUP BY comp
      -- in the consumers would split them into several rows. Duplicate specs
      -- are kept, so `comp` always holds exactly five elements (what
      -- generateCompPage's len(specs) != 5 check and FIND_IN_SET expect).
      SELECT
        R.run_id,
        R.dungeon_id,
        R.season,
        R.keystone_level,
        (R.duration <= DD.upgrade_1_duration) AS timed,
        GROUP_CONCAT(M.spec_id ORDER BY M.spec_id SEPARATOR ',') AS comp
      FROM Mythistone.runs R
        JOIN Mythistone.dungeon_data DD ON DD.dungeon_id = R.dungeon_id
        JOIN Mythistone.run_members RM  ON RM.run_id     = R.run_id
        JOIN Mythistone.members M       ON M.member      = RM.member
      WHERE R.season = v_max_season
        AND R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
        AND R.keystone_level >= v_min_level
        AND M.spec_id IS NOT NULL
      GROUP BY R.run_id, R.dungeon_id, R.season, R.keystone_level, timed
      HAVING COUNT(*) = 5   -- drop runs whose members were only partially collected
    ) c
    GROUP BY c.dungeon_id, c.season, c.keystone_level, c.comp
    ON DUPLICATE KEY UPDATE
      timed_runs    = timed_runs    + VALUES(timed_runs),
      depleted_runs = depleted_runs + VALUES(depleted_runs);

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  -- lock-aware swap: fetch_all_comps scans this table for minutes during page
  -- builds, so a stale reader holding a shared MDL is the expected case here.
  CALL sp_swap_public_table('aggregated_dungeon_comps');
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_dungeon_specs`()
BEGIN
  DECLARE v_min_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_max_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_cur         INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size  INT UNSIGNED DEFAULT 200000;

  CALL sp_agg_session_setup();

  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs;

  DROP TABLE IF EXISTS Mythistone.aggregated_dungeon_specs_new,        Mythistone.aggregated_dungeon_specs_old,
                       Mythistone.aggregated_dungeon_global_specs_new, Mythistone.aggregated_dungeon_global_specs_old;
  CREATE TABLE Mythistone.aggregated_dungeon_specs_new        LIKE Mythistone.aggregated_dungeon_specs;
  CREATE TABLE Mythistone.aggregated_dungeon_global_specs_new LIKE Mythistone.aggregated_dungeon_global_specs;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    -- per-dungeon table: keys >= 12 (matches the old event's final rebuild)
    INSERT INTO Mythistone.aggregated_dungeon_specs_new
      (dungeon_id, season, spec_id, run_count, max_keystone_level, timed_runs, depleted_runs)
    SELECT
      R.dungeon_id,
      R.season,
      M.spec_id,
      COUNT(*) AS run_count,
      MAX(R.keystone_level) AS max_keystone_level,
      SUM(CASE WHEN R.duration <= DD.upgrade_1_duration THEN 1 ELSE 0 END) AS timed_runs,
      SUM(CASE WHEN R.duration <= DD.upgrade_1_duration THEN 0 ELSE 1 END) AS depleted_runs
    FROM Mythistone.runs R
    JOIN Mythistone.dungeon_data DD ON DD.dungeon_id = R.dungeon_id
    JOIN Mythistone.run_members RM ON R.run_id = RM.run_id
    JOIN Mythistone.members M ON RM.member = M.member
    WHERE R.keystone_level >= 12
      AND R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
    GROUP BY R.dungeon_id, R.season, M.spec_id
    ON DUPLICATE KEY UPDATE
      run_count          = run_count + VALUES(run_count),
      max_keystone_level = GREATEST(max_keystone_level, VALUES(max_keystone_level)),
      timed_runs         = timed_runs + VALUES(timed_runs),
      depleted_runs      = depleted_runs + VALUES(depleted_runs);

    -- global denominators: keys >= 10 (matches the old event's first pass,
    -- whose per-dungeon sums fed the global table)
    INSERT INTO Mythistone.aggregated_dungeon_global_specs_new
      (season, spec_id, run_count)
    SELECT
      R.season, M.spec_id, COUNT(*) AS run_count
    FROM Mythistone.runs R
    JOIN Mythistone.run_members RM ON R.run_id = RM.run_id
    JOIN Mythistone.members M ON RM.member = M.member
    WHERE R.keystone_level >= 10
      AND R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
    GROUP BY R.season, M.spec_id
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count);

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE
    Mythistone.aggregated_dungeon_specs            TO Mythistone.aggregated_dungeon_specs_old,
    Mythistone.aggregated_dungeon_specs_new        TO Mythistone.aggregated_dungeon_specs,
    Mythistone.aggregated_dungeon_global_specs     TO Mythistone.aggregated_dungeon_global_specs_old,
    Mythistone.aggregated_dungeon_global_specs_new TO Mythistone.aggregated_dungeon_global_specs;
  DROP TABLE Mythistone.aggregated_dungeon_specs_old,
             Mythistone.aggregated_dungeon_global_specs_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_embellishments`()
BEGIN
  -- Full rebuild of the trailing 14 days into a shadow table, then atomic swap
  -- (same rationale as sp_agg_missives: purged detail rows make a watermark sum
  -- over-count, so rebuild the 14-day window every night).
  DECLARE v_cutoff_ms  BIGINT       DEFAULT 0;
  DECLARE v_min_run    INT UNSIGNED DEFAULT 1;
  DECLARE v_max_run    INT UNSIGNED DEFAULT 0;
  DECLARE v_cur        INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size INT UNSIGNED DEFAULT 200000;

  CALL sp_agg_session_setup();

  SET v_cutoff_ms = UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000;

  SELECT COALESCE(MIN(run_id), 1), COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE `timestamp` > v_cutoff_ms;

  DROP TABLE IF EXISTS Mythistone.aggregated_embellishments_new, Mythistone.aggregated_embellishments_old;
  CREATE TABLE Mythistone.aggregated_embellishments_new LIKE Mythistone.aggregated_embellishments;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_embellishments_new
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
        JOIN Mythistone.bonus_sets B      ON B.set_id = EQ.bonus_set_id
        JOIN Mythistone.embellishments EM ON EM.bonus_id = B.bonus_id
      WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
        AND R.`timestamp` > v_cutoff_ms
      GROUP BY R.run_id, EQ.equipment_id, EM.item_id
    ) t
    GROUP BY
      t.spec_id, t.season, t.dungeon_id, t.keystone_level, t.upgrade_tier,
      t.hero_talent_id, t.item_id
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count);

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE Mythistone.aggregated_embellishments     TO Mythistone.aggregated_embellishments_old,
               Mythistone.aggregated_embellishments_new TO Mythistone.aggregated_embellishments;
  DROP TABLE Mythistone.aggregated_embellishments_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_enchantments_slot_group`()
BEGIN
  DECLARE v_cutoff_ms  BIGINT       DEFAULT 0;
  DECLARE v_min_run    INT UNSIGNED DEFAULT 1;
  DECLARE v_max_run    INT UNSIGNED DEFAULT 0;
  DECLARE v_cur        INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size INT UNSIGNED DEFAULT 200000;

  CALL sp_agg_session_setup();

  SET v_cutoff_ms = UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000;

  SELECT COALESCE(MIN(run_id), 1), COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE `timestamp` > v_cutoff_ms;

  DROP TABLE IF EXISTS Mythistone.aggregated_enchantments_slot_group_new,
                       Mythistone.aggregated_enchantments_slot_group_old;
  CREATE TABLE Mythistone.aggregated_enchantments_slot_group_new
    LIKE Mythistone.aggregated_enchantments_slot_group;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_enchantments_slot_group_new
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
    WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
      AND R.`timestamp` > v_cutoff_ms
    GROUP BY
      M.spec_id, R.season, R.dungeon_id, R.keystone_level, upgrade_tier,
      COALESCE(M.hero_talent_id,0), COALESCE(SGM.slot_group, EQ.slot), E.enchantment_id
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count);

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE Mythistone.aggregated_enchantments_slot_group     TO Mythistone.aggregated_enchantments_slot_group_old,
               Mythistone.aggregated_enchantments_slot_group_new TO Mythistone.aggregated_enchantments_slot_group;
  DROP TABLE Mythistone.aggregated_enchantments_slot_group_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_equipment`()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE current_dungeon VARCHAR(100);
  DECLARE cur CURSOR FOR
    SELECT DISTINCT dungeon_id
    FROM Mythistone.runs
    WHERE `timestamp` > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.aggregated_equipment_new, Mythistone.aggregated_equipment_old;
  CREATE TABLE Mythistone.aggregated_equipment_new LIKE Mythistone.aggregated_equipment;

  OPEN cur;

  read_loop: LOOP
    FETCH cur INTO current_dungeon;
    IF done THEN
      LEAVE read_loop;
    END IF;

    INSERT INTO Mythistone.aggregated_equipment_new
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
      AND R.`timestamp` > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000
      AND EQ.item_id IS NOT NULL
    GROUP BY
      M.spec_id, R.season, R.dungeon_id, R.keystone_level, upgrade_tier,
      COALESCE(M.hero_talent_id,0), EQ.item_id, EQ.slot;

  END LOOP;

  CLOSE cur;

  RENAME TABLE Mythistone.aggregated_equipment     TO Mythistone.aggregated_equipment_old,
               Mythistone.aggregated_equipment_new TO Mythistone.aggregated_equipment;
  DROP TABLE Mythistone.aggregated_equipment_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_eq_comps`()
BEGIN
  DECLARE v_min_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_max_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_cur         INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size  INT UNSIGNED DEFAULT 200000; -- tune: 200K runs × 5 members = ~1M rows/pass

  CALL sp_agg_session_setup();

  -- Both aggregations are full rebuilds: equipped gear data older than 2 weeks
  -- is discarded, so purged runs must drop out of the aggregates too (a
  -- watermark-incremental approach would keep their counts forever).
  -- Batching on run_id is safe because every comp group is scoped to a single
  -- (run, member) and can never span a batch boundary; the ON DUPLICATE KEY
  -- UPDATE accumulates the per-batch partial aggregates.

  -- run_id boundaries (NULL-safe: if no runs exist, the WHILE never executes)
  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs;

  DROP TABLE IF EXISTS Mythistone.aggregated_embellishment_comps_new, Mythistone.aggregated_embellishment_comps_old,
                       Mythistone.aggregated_crafted_comps_new,       Mythistone.aggregated_crafted_comps_old,
                       Mythistone.aggregated_tier_set_comps_new,      Mythistone.aggregated_tier_set_comps_old,
                       Mythistone.aggregated_gem_comps_new,           Mythistone.aggregated_gem_comps_old,
                       Mythistone.aggregated_enchant_comps_new,       Mythistone.aggregated_enchant_comps_old;
  CREATE TABLE Mythistone.aggregated_embellishment_comps_new LIKE Mythistone.aggregated_embellishment_comps;
  CREATE TABLE Mythistone.aggregated_crafted_comps_new       LIKE Mythistone.aggregated_crafted_comps;
  CREATE TABLE Mythistone.aggregated_tier_set_comps_new      LIKE Mythistone.aggregated_tier_set_comps;
  CREATE TABLE Mythistone.aggregated_gem_comps_new           LIKE Mythistone.aggregated_gem_comps;
  CREATE TABLE Mythistone.aggregated_enchant_comps_new       LIKE Mythistone.aggregated_enchant_comps;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    -- 1. Embellishment comps: canonical sorted list of embellishment item_ids per (run, member)
    INSERT LOW_PRIORITY INTO Mythistone.aggregated_embellishment_comps_new
      (spec_id, season, hero_talent_id, comp, run_count, max_timed_key, max_depleted_key)
    SELECT
      c.spec_id,
      c.season,
      c.hero_talent_id,
      c.comp,
      COUNT(*) AS run_count,
      MAX(IF(c.timed, c.keystone_level, 0)) AS max_timed_key,
      MAX(IF(c.timed, 0, c.keystone_level)) AS max_depleted_key
    FROM (
      SELECT
        p.spec_id,
        p.season,
        p.hero_talent_id,
        p.keystone_level,
        p.timed,
        GROUP_CONCAT(p.item_id ORDER BY p.item_id SEPARATOR ',') AS comp
      FROM (
        SELECT DISTINCT
          R.run_id,
          RM.member,
          M.spec_id,
          COALESCE(R.season, 0) AS season,
          COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
          R.keystone_level,
          (R.duration <= DD.upgrade_1_duration) AS timed,
          EQ.equipment_id,
          EM.item_id
        FROM Mythistone.runs R
          JOIN Mythistone.dungeon_data DD   ON R.dungeon_id = DD.dungeon_id
          JOIN Mythistone.run_members RM    ON R.run_id = RM.run_id
          JOIN Mythistone.members M         ON RM.member = M.member
          JOIN Mythistone.equipment EQ      ON M.member = EQ.member
          JOIN Mythistone.bonus_sets B      ON B.set_id = EQ.bonus_set_id
          JOIN Mythistone.embellishments EM ON EM.bonus_id = B.bonus_id
        WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
      ) p
      GROUP BY p.run_id, p.member, p.spec_id, p.season, p.hero_talent_id,
               p.keystone_level, p.timed
    ) c
    GROUP BY c.spec_id, c.season, c.hero_talent_id, c.comp
    ON DUPLICATE KEY UPDATE
      run_count        = run_count + VALUES(run_count),
      max_timed_key    = GREATEST(max_timed_key, VALUES(max_timed_key)),
      max_depleted_key = GREATEST(max_depleted_key, VALUES(max_depleted_key));

    -- 2. Crafted item comps: canonical sorted list of crafted item_ids per (run, member)
    INSERT LOW_PRIORITY INTO Mythistone.aggregated_crafted_comps_new
      (spec_id, season, hero_talent_id, comp, run_count, max_timed_key, max_depleted_key)
    SELECT
      c.spec_id,
      c.season,
      c.hero_talent_id,
      c.comp,
      COUNT(*) AS run_count,
      MAX(IF(c.timed, c.keystone_level, 0)) AS max_timed_key,
      MAX(IF(c.timed, 0, c.keystone_level)) AS max_depleted_key
    FROM (
      SELECT
        p.spec_id,
        p.season,
        p.hero_talent_id,
        p.keystone_level,
        p.timed,
        GROUP_CONCAT(p.item_id ORDER BY p.item_id SEPARATOR ',') AS comp
      FROM (
        SELECT
          R.run_id,
          RM.member,
          M.spec_id,
          COALESCE(R.season, 0) AS season,
          COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
          R.keystone_level,
          (R.duration <= DD.upgrade_1_duration) AS timed,
          EQ.equipment_id,
          EQ.item_id
        FROM Mythistone.runs R
          JOIN Mythistone.dungeon_data DD    ON R.dungeon_id = DD.dungeon_id
          JOIN Mythistone.run_members RM     ON R.run_id = RM.run_id
          JOIN Mythistone.members M          ON RM.member = M.member
          JOIN Mythistone.equipment EQ       ON M.member = EQ.member
          JOIN Mythistone.crafted_item_ids CII ON EQ.item_id = CII.item_id
        WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
      ) p
      GROUP BY p.run_id, p.member, p.spec_id, p.season, p.hero_talent_id,
               p.keystone_level, p.timed
    ) c
    GROUP BY c.spec_id, c.season, c.hero_talent_id, c.comp
    ON DUPLICATE KEY UPDATE
      run_count        = run_count + VALUES(run_count),
      max_timed_key    = GREATEST(max_timed_key, VALUES(max_timed_key)),
      max_depleted_key = GREATEST(max_depleted_key, VALUES(max_depleted_key));

    -- 3. Set comps: canonical sorted list of equipped set piece item_ids per (run, member).
    --    Only sets the member wears at least 2 pieces of count (set bonuses start
    --    at 2pc); lone pieces of another set would otherwise pollute the comp.
    INSERT LOW_PRIORITY INTO Mythistone.aggregated_tier_set_comps_new
      (spec_id, season, hero_talent_id, comp, run_count, max_timed_key, max_depleted_key)
    SELECT
      c.spec_id,
      c.season,
      c.hero_talent_id,
      c.comp,
      COUNT(*) AS run_count,
      MAX(IF(c.timed, c.keystone_level, 0)) AS max_timed_key,
      MAX(IF(c.timed, 0, c.keystone_level)) AS max_depleted_key
    FROM (
      SELECT
        q.spec_id,
        q.season,
        q.hero_talent_id,
        q.keystone_level,
        q.timed,
        GROUP_CONCAT(q.item_id ORDER BY q.item_id SEPARATOR ',') AS comp
      FROM (
        SELECT
          p.*,
          COUNT(*) OVER (PARTITION BY p.run_id, p.member, p.item_set_id) AS set_piece_count
        FROM (
          SELECT
            R.run_id,
            RM.member,
            M.spec_id,
            COALESCE(R.season, 0) AS season,
            COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
            R.keystone_level,
            (R.duration <= DD.upgrade_1_duration) AS timed,
            EQ.equipment_id,
            EQ.item_id,
            TSI.item_set_id
          FROM Mythistone.runs R
            JOIN Mythistone.dungeon_data DD    ON R.dungeon_id = DD.dungeon_id
            JOIN Mythistone.run_members RM     ON R.run_id = RM.run_id
            JOIN Mythistone.members M          ON RM.member = M.member
            JOIN Mythistone.equipment EQ       ON M.member = EQ.member
            JOIN Mythistone.tier_set_items TSI ON EQ.item_id = TSI.item_id
          WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
        ) p
      ) q
      WHERE q.set_piece_count >= 2
      GROUP BY q.run_id, q.member, q.spec_id, q.season, q.hero_talent_id,
               q.keystone_level, q.timed
    ) c
    GROUP BY c.spec_id, c.season, c.hero_talent_id, c.comp
    ON DUPLICATE KEY UPDATE
      run_count        = run_count + VALUES(run_count),
      max_timed_key    = GREATEST(max_timed_key, VALUES(max_timed_key)),
      max_depleted_key = GREATEST(max_depleted_key, VALUES(max_depleted_key));

    -- 4. Gem comps: canonical multiset of every socket gem item_id a member wears
    --    across their whole gear set per (run, member). Repeats are kept (3x the
    --    same gem stays three entries); ORDER BY socket_item_id inside the
    --    GROUP_CONCAT normalises position so the same multiset collapses to one row.
    INSERT LOW_PRIORITY INTO Mythistone.aggregated_gem_comps_new
      (spec_id, season, hero_talent_id, comp, run_count, max_timed_key, max_depleted_key)
    SELECT
      c.spec_id,
      c.season,
      c.hero_talent_id,
      c.comp,
      COUNT(*) AS run_count,
      MAX(IF(c.timed, c.keystone_level, 0)) AS max_timed_key,
      MAX(IF(c.timed, 0, c.keystone_level)) AS max_depleted_key
    FROM (
      SELECT
        p.spec_id,
        p.season,
        p.hero_talent_id,
        p.keystone_level,
        p.timed,
        GROUP_CONCAT(p.socket_item_id ORDER BY p.socket_item_id SEPARATOR ',') AS comp
      FROM (
        SELECT
          R.run_id,
          RM.member,
          M.spec_id,
          COALESCE(R.season, 0) AS season,
          COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
          R.keystone_level,
          (R.duration <= DD.upgrade_1_duration) AS timed,
          SO.socket_id_pk,
          CAST(SO.socket_item_id AS UNSIGNED) AS socket_item_id
        FROM Mythistone.runs R
          JOIN Mythistone.dungeon_data DD ON R.dungeon_id = DD.dungeon_id
          JOIN Mythistone.run_members RM  ON R.run_id = RM.run_id
          JOIN Mythistone.members M       ON RM.member = M.member
          JOIN Mythistone.equipment EQ    ON M.member = EQ.member
          JOIN Mythistone.sockets SO      ON SO.equipment_id = EQ.equipment_id
        WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
      ) p
      GROUP BY p.run_id, p.member, p.spec_id, p.season, p.hero_talent_id,
               p.keystone_level, p.timed
    ) c
    GROUP BY c.spec_id, c.season, c.hero_talent_id, c.comp
    ON DUPLICATE KEY UPDATE
      run_count        = run_count + VALUES(run_count),
      max_timed_key    = GREATEST(max_timed_key, VALUES(max_timed_key)),
      max_depleted_key = GREATEST(max_depleted_key, VALUES(max_depleted_key));

    -- 5. Enchant comps: canonical multiset of every enchantment_id a member wears
    --    across their whole gear set per (run, member). Same repeat/ordering rules
    --    as the gem comps above.
    INSERT LOW_PRIORITY INTO Mythistone.aggregated_enchant_comps_new
      (spec_id, season, hero_talent_id, comp, run_count, max_timed_key, max_depleted_key)
    SELECT
      c.spec_id,
      c.season,
      c.hero_talent_id,
      c.comp,
      COUNT(*) AS run_count,
      MAX(IF(c.timed, c.keystone_level, 0)) AS max_timed_key,
      MAX(IF(c.timed, 0, c.keystone_level)) AS max_depleted_key
    FROM (
      SELECT
        p.spec_id,
        p.season,
        p.hero_talent_id,
        p.keystone_level,
        p.timed,
        GROUP_CONCAT(p.enchantment_id ORDER BY p.enchantment_id SEPARATOR ',') AS comp
      FROM (
        SELECT
          R.run_id,
          RM.member,
          M.spec_id,
          COALESCE(R.season, 0) AS season,
          COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
          R.keystone_level,
          (R.duration <= DD.upgrade_1_duration) AS timed,
          E.enchantment_id_pk,
          CAST(E.enchantment_id AS UNSIGNED) AS enchantment_id
        FROM Mythistone.runs R
          JOIN Mythistone.dungeon_data DD  ON R.dungeon_id = DD.dungeon_id
          JOIN Mythistone.run_members RM   ON R.run_id = RM.run_id
          JOIN Mythistone.members M        ON RM.member = M.member
          JOIN Mythistone.equipment EQ     ON M.member = EQ.member
          JOIN Mythistone.enchantments E   ON E.equipment_id = EQ.equipment_id
        WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
      ) p
      GROUP BY p.run_id, p.member, p.spec_id, p.season, p.hero_talent_id,
               p.keystone_level, p.timed
    ) c
    GROUP BY c.spec_id, c.season, c.hero_talent_id, c.comp
    ON DUPLICATE KEY UPDATE
      run_count        = run_count + VALUES(run_count),
      max_timed_key    = GREATEST(max_timed_key, VALUES(max_timed_key)),
      max_depleted_key = GREATEST(max_depleted_key, VALUES(max_depleted_key));

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE
    Mythistone.aggregated_embellishment_comps     TO Mythistone.aggregated_embellishment_comps_old,
    Mythistone.aggregated_embellishment_comps_new TO Mythistone.aggregated_embellishment_comps,
    Mythistone.aggregated_crafted_comps           TO Mythistone.aggregated_crafted_comps_old,
    Mythistone.aggregated_crafted_comps_new       TO Mythistone.aggregated_crafted_comps,
    Mythistone.aggregated_tier_set_comps          TO Mythistone.aggregated_tier_set_comps_old,
    Mythistone.aggregated_tier_set_comps_new      TO Mythistone.aggregated_tier_set_comps,
    Mythistone.aggregated_gem_comps               TO Mythistone.aggregated_gem_comps_old,
    Mythistone.aggregated_gem_comps_new           TO Mythistone.aggregated_gem_comps,
    Mythistone.aggregated_enchant_comps           TO Mythistone.aggregated_enchant_comps_old,
    Mythistone.aggregated_enchant_comps_new       TO Mythistone.aggregated_enchant_comps;
  DROP TABLE Mythistone.aggregated_embellishment_comps_old,
             Mythistone.aggregated_crafted_comps_old,
             Mythistone.aggregated_tier_set_comps_old,
             Mythistone.aggregated_gem_comps_old,
             Mythistone.aggregated_enchant_comps_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_global`()
BEGIN
  CALL sp_agg_session_setup();

  -- 1. Equipment
  DROP TABLE IF EXISTS Mythistone.global_aggregated_equipment_new, Mythistone.global_aggregated_equipment_old;
  CREATE TABLE Mythistone.global_aggregated_equipment_new LIKE Mythistone.global_aggregated_equipment;
  INSERT INTO Mythistone.global_aggregated_equipment_new (spec_id, season, item_id, slot, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, item_id, slot, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_equipment
  GROUP BY spec_id, season, item_id, slot;
  CALL sp_swap_public_table('global_aggregated_equipment');

  -- 2. Enchantments
  DROP TABLE IF EXISTS Mythistone.global_aggregated_enchantments_slot_group_new, Mythistone.global_aggregated_enchantments_slot_group_old;
  CREATE TABLE Mythistone.global_aggregated_enchantments_slot_group_new LIKE Mythistone.global_aggregated_enchantments_slot_group;
  INSERT INTO Mythistone.global_aggregated_enchantments_slot_group_new (spec_id, season, slot_group, enchantment_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, slot_group, enchantment_id, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_enchantments_slot_group
  GROUP BY spec_id, season, slot_group, enchantment_id;
  CALL sp_swap_public_table('global_aggregated_enchantments_slot_group');

  -- 3. Sockets (reads detail tables directly; bounded by the 14-day purge of
  --    equipment/sockets rows)
  DROP TABLE IF EXISTS Mythistone.global_aggregated_item_sockets_new, Mythistone.global_aggregated_item_sockets_old;
  CREATE TABLE Mythistone.global_aggregated_item_sockets_new LIKE Mythistone.global_aggregated_item_sockets;
  INSERT INTO Mythistone.global_aggregated_item_sockets_new (spec_id, season, item_id, socket_item_id, run_count, max_timed_key, max_depleted_key)
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
  CALL sp_swap_public_table('global_aggregated_item_sockets');

  -- 4. Missives
  DROP TABLE IF EXISTS Mythistone.global_aggregated_missives_new, Mythistone.global_aggregated_missives_old;
  CREATE TABLE Mythistone.global_aggregated_missives_new LIKE Mythistone.global_aggregated_missives;
  INSERT INTO Mythistone.global_aggregated_missives_new (spec_id, season, item_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, item_id, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_missives
  GROUP BY spec_id, season, item_id;
  CALL sp_swap_public_table('global_aggregated_missives');

  -- 5. Embellishments
  DROP TABLE IF EXISTS Mythistone.global_aggregated_embellishments_new, Mythistone.global_aggregated_embellishments_old;
  CREATE TABLE Mythistone.global_aggregated_embellishments_new LIKE Mythistone.global_aggregated_embellishments;
  INSERT INTO Mythistone.global_aggregated_embellishments_new (spec_id, season, item_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, item_id, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_embellishments
  GROUP BY spec_id, season, item_id;
  CALL sp_swap_public_table('global_aggregated_embellishments');

  -- 6. Hero Talents (last 14 days — same window as equipment/talent data;
  --    aggregated_spec is season-wide, so build directly from runs instead)
  DROP TABLE IF EXISTS Mythistone.global_aggregated_hero_talent_overview_new, Mythistone.global_aggregated_hero_talent_overview_old;
  CREATE TABLE Mythistone.global_aggregated_hero_talent_overview_new LIKE Mythistone.global_aggregated_hero_talent_overview;
  INSERT INTO Mythistone.global_aggregated_hero_talent_overview_new (spec_id, hero_talent_id, run_count, max_timed_key, max_depleted_key)
  SELECT
    M.spec_id,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    COUNT(DISTINCT R.run_id) AS run_count,
    MAX(IF(R.duration <= DD.upgrade_1_duration, R.keystone_level, 0)),
    MAX(IF(R.duration <= DD.upgrade_1_duration, 0, R.keystone_level))
  FROM Mythistone.runs R
    JOIN Mythistone.dungeon_data DD ON R.dungeon_id = DD.dungeon_id
    JOIN Mythistone.run_members RM  ON R.run_id = RM.run_id
    JOIN Mythistone.members M       ON RM.member = M.member
  WHERE R.timestamp > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000
    AND M.spec_id IS NOT NULL
    AND R.keystone_level IS NOT NULL
  GROUP BY M.spec_id, COALESCE(M.hero_talent_id, 0);
  CALL sp_swap_public_table('global_aggregated_hero_talent_overview');

  -- 7. Loadouts
  DROP TABLE IF EXISTS Mythistone.global_aggregated_loadout_data_new, Mythistone.global_aggregated_loadout_data_old;
  CREATE TABLE Mythistone.global_aggregated_loadout_data_new LIKE Mythistone.global_aggregated_loadout_data;
  INSERT INTO Mythistone.global_aggregated_loadout_data_new (spec_id, season, hero_talent_id, loadout, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, hero_talent_id_key AS hero_talent_id, loadout_key AS loadout, SUM(run_count),
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_loadout_data
  WHERE loadout_key != '<NULL>'
  GROUP BY spec_id, season, hero_talent_id_key, loadout_key;
  CALL sp_swap_public_table('global_aggregated_loadout_data');

  -- 8. Global Equipment without Slot (for true max keys per item)
  DROP TABLE IF EXISTS Mythistone.global_aggregated_items_new, Mythistone.global_aggregated_items_old;
  CREATE TABLE Mythistone.global_aggregated_items_new LIKE Mythistone.global_aggregated_items;
  INSERT INTO Mythistone.global_aggregated_items_new (spec_id, season, item_id, run_count, max_timed_key, max_depleted_key)
  SELECT
    spec_id,
    season,
    item_id,
    SUM(run_count),
    MAX(CASE WHEN upgrade_tier != 'depleted' THEN keystone_level ELSE 0 END),
    MAX(CASE WHEN upgrade_tier = 'depleted' THEN keystone_level ELSE 0 END)
  FROM Mythistone.aggregated_equipment
  GROUP BY spec_id, season, item_id;
  CALL sp_swap_public_table('global_aggregated_items');

  -- 9. Crafted items
  DROP TABLE IF EXISTS Mythistone.global_aggregated_crafted_items_new, Mythistone.global_aggregated_crafted_items_old;
  CREATE TABLE Mythistone.global_aggregated_crafted_items_new LIKE Mythistone.global_aggregated_crafted_items;
  INSERT INTO Mythistone.global_aggregated_crafted_items_new (spec_id, season, item_id, run_count, max_timed_key, max_depleted_key)
  SELECT spec_id, season, item_id, SUM(run_count) AS run_count,
         MAX(IF(upgrade_tier IN ('1','2','3'), keystone_level, 0)),
         MAX(IF(upgrade_tier = 'depleted', keystone_level, 0))
  FROM Mythistone.aggregated_crafted_items
  GROUP BY spec_id, season, item_id;
  CALL sp_swap_public_table('global_aggregated_crafted_items');
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_global_bonus_lists`()
BEGIN
  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.global_aggregated_bonus_lists_new, Mythistone.global_aggregated_bonus_lists_old;
  CREATE TABLE Mythistone.global_aggregated_bonus_lists_new LIKE Mythistone.global_aggregated_bonus_lists;

  INSERT INTO Mythistone.global_aggregated_bonus_lists_new (spec_id, season, item_id, bonus_list, run_count)
  SELECT spec_id, season, item_id, bonus_list, SUM(run_count)
  FROM Mythistone.aggregated_bonus_lists
  GROUP BY spec_id, season, item_id, bonus_list;

  CALL sp_swap_public_table('global_aggregated_bonus_lists');
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_hero_talent`()
BEGIN
  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.aggregated_hero_talent_new, Mythistone.aggregated_hero_talent_old;
  CREATE TABLE Mythistone.aggregated_hero_talent_new LIKE Mythistone.aggregated_hero_talent;

  INSERT INTO Mythistone.aggregated_hero_talent_new
    (spec_id, season, dungeon_id, hero_talent_id, talent_id, run_count, avg_rank)
  SELECT
    M.spec_id,
    R.season,
    R.dungeon_id,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    HT.talent_id,
    COUNT(*) AS run_count,
    AVG(HT.rank) AS avg_rank
  FROM Mythistone.runs R
    JOIN Mythistone.dungeon_data DD  ON R.dungeon_id = DD.dungeon_id
    JOIN Mythistone.run_members RM   ON R.run_id     = RM.run_id
    JOIN Mythistone.members M        ON RM.member    = M.member
    JOIN Mythistone.talent_sets HT   ON HT.set_id    = M.talent_set_id AND HT.tree = 2
  WHERE R.`timestamp` > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000
  GROUP BY
    M.spec_id, R.season, R.dungeon_id,
    COALESCE(M.hero_talent_id, 0), HT.talent_id;

  RENAME TABLE Mythistone.aggregated_hero_talent     TO Mythistone.aggregated_hero_talent_old,
               Mythistone.aggregated_hero_talent_new TO Mythistone.aggregated_hero_talent;
  DROP TABLE Mythistone.aggregated_hero_talent_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_key_throughput`()
BEGIN
  DECLARE v_max_season  INT          DEFAULT 0;
  DECLARE v_min_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_max_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_cur         INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size  INT UNSIGNED DEFAULT 200000; -- run_ids per pass

  CALL sp_agg_session_setup();

  -- 1. Resolve current season
  SELECT MAX(season) INTO v_max_season FROM Mythistone.runs;

  -- 2. Find the run_id boundaries for this season
  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE season = v_max_season;

  DROP TABLE IF EXISTS Mythistone.aggregated_key_throughput_new, Mythistone.aggregated_key_throughput_old;
  CREATE TABLE Mythistone.aggregated_key_throughput_new LIKE Mythistone.aggregated_key_throughput;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_key_throughput_new
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

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE Mythistone.aggregated_key_throughput     TO Mythistone.aggregated_key_throughput_old,
               Mythistone.aggregated_key_throughput_new TO Mythistone.aggregated_key_throughput;
  DROP TABLE Mythistone.aggregated_key_throughput_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_loadout_data`()
BEGIN
  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.aggregated_loadout_data_new, Mythistone.aggregated_loadout_data_old;
  CREATE TABLE Mythistone.aggregated_loadout_data_new LIKE Mythistone.aggregated_loadout_data;

  INSERT INTO Mythistone.aggregated_loadout_data_new
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
  FROM Mythistone.runs r
  JOIN Mythistone.run_members rm ON rm.run_id = r.run_id
  JOIN Mythistone.members m ON m.member = rm.member
  JOIN Mythistone.dungeon_data dd ON dd.dungeon_id = r.dungeon_id
  WHERE r.`timestamp` > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000
    AND m.loadout IS NOT NULL
  GROUP BY m.spec_id, r.season, r.dungeon_id, r.keystone_level, upgrade_tier, COALESCE(m.hero_talent_id, 0), m.loadout;

  RENAME TABLE Mythistone.aggregated_loadout_data     TO Mythistone.aggregated_loadout_data_old,
               Mythistone.aggregated_loadout_data_new TO Mythistone.aggregated_loadout_data;
  DROP TABLE Mythistone.aggregated_loadout_data_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_missives`()
BEGIN
  -- Full rebuild of the trailing 14 days into a shadow table, then atomic swap.
  -- Equipment/socket rows are purged after ~2 weeks, so a watermark-incremental
  -- sum would keep counts from purged runs that never decay; rebuilding the same
  -- 14-day window every night keeps this consistent with equipment/enchantments.
  DECLARE v_cutoff_ms  BIGINT       DEFAULT 0;
  DECLARE v_min_run    INT UNSIGNED DEFAULT 1;
  DECLARE v_max_run    INT UNSIGNED DEFAULT 0;
  DECLARE v_cur        INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size INT UNSIGNED DEFAULT 200000;

  CALL sp_agg_session_setup();

  SET v_cutoff_ms = UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000;

  SELECT COALESCE(MIN(run_id), 1), COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE `timestamp` > v_cutoff_ms;

  DROP TABLE IF EXISTS Mythistone.aggregated_missives_new, Mythistone.aggregated_missives_old;
  CREATE TABLE Mythistone.aggregated_missives_new LIKE Mythistone.aggregated_missives;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_missives_new
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
        JOIN Mythistone.bonus_sets B      ON B.set_id = EQ.bonus_set_id
        JOIN Mythistone.missives MS       ON MS.bonus_id = B.bonus_id
      WHERE R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
        AND R.`timestamp` > v_cutoff_ms
      GROUP BY R.run_id, EQ.equipment_id, MS.item_id
    ) t
    GROUP BY
      t.spec_id, t.season, t.dungeon_id, t.keystone_level, t.upgrade_tier,
      t.hero_talent_id, t.item_id
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count);

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE Mythistone.aggregated_missives     TO Mythistone.aggregated_missives_old,
               Mythistone.aggregated_missives_new TO Mythistone.aggregated_missives;
  DROP TABLE Mythistone.aggregated_missives_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_runs_per_dungeon_per_level`()
BEGIN
  DECLARE v_max_season  INT          DEFAULT 0;
  DECLARE v_min_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_max_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_cur         INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size  INT UNSIGNED DEFAULT 200000; -- run_ids per pass

  CALL sp_agg_session_setup();

  -- 1. Resolve current season
  SELECT MAX(season) INTO v_max_season FROM Mythistone.runs;

  -- 2. Find the run_id boundaries for this season
  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE season = v_max_season;

  DROP TABLE IF EXISTS Mythistone.aggregated_runs_per_dungeon_per_level_new, Mythistone.aggregated_runs_per_dungeon_per_level_old;
  CREATE TABLE Mythistone.aggregated_runs_per_dungeon_per_level_new LIKE Mythistone.aggregated_runs_per_dungeon_per_level;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_runs_per_dungeon_per_level_new
      (season, dungeon_id, keystone_level, tier_3, tier_2, tier_1, depleted, total_runs)
    SELECT
      R.season,
      R.dungeon_id,
      R.keystone_level,
      SUM(CASE WHEN R.duration IS NOT NULL
                   AND DD.upgrade_3_duration IS NOT NULL
                   AND R.duration <= DD.upgrade_3_duration THEN 1 ELSE 0 END) AS tier_3,
      SUM(CASE WHEN R.duration IS NOT NULL
                   AND DD.upgrade_2_duration IS NOT NULL
                   AND R.duration <= DD.upgrade_2_duration
                   AND NOT (R.duration <= DD.upgrade_3_duration) THEN 1 ELSE 0 END) AS tier_2,
      SUM(CASE WHEN R.duration IS NOT NULL
                   AND DD.upgrade_1_duration IS NOT NULL
                   AND R.duration <= DD.upgrade_1_duration
                   AND NOT (R.duration <= DD.upgrade_2_duration) THEN 1 ELSE 0 END) AS tier_1,
      SUM(CASE WHEN R.duration IS NULL
                   OR (DD.upgrade_1_duration IS NOT NULL AND R.duration > DD.upgrade_1_duration)
                   THEN 1 ELSE 0 END) AS depleted,
      COUNT(*) AS total_runs
    FROM Mythistone.runs R
    JOIN Mythistone.dungeon_data DD ON DD.dungeon_id = R.dungeon_id
    WHERE R.season = v_max_season
      AND R.run_id BETWEEN v_cur AND (v_cur + v_batch_size - 1)
    GROUP BY R.season, R.dungeon_id, R.keystone_level
    ON DUPLICATE KEY UPDATE
      tier_3     = tier_3     + VALUES(tier_3),
      tier_2     = tier_2     + VALUES(tier_2),
      tier_1     = tier_1     + VALUES(tier_1),
      depleted   = depleted   + VALUES(depleted),
      total_runs = total_runs + VALUES(total_runs);

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE Mythistone.aggregated_runs_per_dungeon_per_level     TO Mythistone.aggregated_runs_per_dungeon_per_level_old,
               Mythistone.aggregated_runs_per_dungeon_per_level_new TO Mythistone.aggregated_runs_per_dungeon_per_level;
  DROP TABLE Mythistone.aggregated_runs_per_dungeon_per_level_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_session_setup`()
BEGIN
  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;
  SET SESSION lock_wait_timeout = 60;
  SET SESSION innodb_lock_wait_timeout = 30;
  SET SESSION sort_buffer_size    = 64 * 1024 * 1024;
  SET SESSION tmp_table_size      = 64 * 1024 * 1024;
  SET SESSION max_heap_table_size = 64 * 1024 * 1024;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_spec`()
BEGIN
  DECLARE v_max_season  INT          DEFAULT 0;
  DECLARE v_min_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_max_run     INT UNSIGNED DEFAULT 0;
  DECLARE v_cur         INT UNSIGNED DEFAULT 0;
  DECLARE v_batch_size  INT UNSIGNED DEFAULT 200000; -- tune: 200K runs × 5 members = ~1M rows/pass

  CALL sp_agg_session_setup();

  -- 1. Resolve current season
  SELECT MAX(season) INTO v_max_season FROM Mythistone.runs;

  -- 2. Find the run_id boundaries for this season
  --    (NULL-safe: if no runs exist for the season, the WHILE never executes)
  SELECT COALESCE(MIN(run_id), 1),
         COALESCE(MAX(run_id), 0)
    INTO v_min_run, v_max_run
  FROM Mythistone.runs
  WHERE season = v_max_season;

  DROP TABLE IF EXISTS Mythistone.aggregated_spec_new, Mythistone.aggregated_spec_old;
  CREATE TABLE Mythistone.aggregated_spec_new LIKE Mythistone.aggregated_spec;

  SET v_cur = v_min_run;

  WHILE v_cur <= v_max_run DO

    INSERT INTO Mythistone.aggregated_spec_new
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

    SET v_cur = v_cur + v_batch_size;

  END WHILE;

  RENAME TABLE Mythistone.aggregated_spec     TO Mythistone.aggregated_spec_old,
               Mythistone.aggregated_spec_new TO Mythistone.aggregated_spec;
  DROP TABLE Mythistone.aggregated_spec_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_spec_talent`()
BEGIN
  CALL sp_agg_session_setup();

  DROP TABLE IF EXISTS Mythistone.aggregated_spec_talent_new, Mythistone.aggregated_spec_talent_old;
  CREATE TABLE Mythistone.aggregated_spec_talent_new LIKE Mythistone.aggregated_spec_talent;

  INSERT INTO Mythistone.aggregated_spec_talent_new
    (spec_id, season, dungeon_id, hero_talent_id, talent_id, run_count, avg_rank)
  SELECT
    M.spec_id,
    R.season,
    R.dungeon_id,
    COALESCE(M.hero_talent_id, 0) AS hero_talent_id,
    ST.talent_id,
    COUNT(*) AS run_count,
    AVG(ST.rank) AS avg_rank
  FROM Mythistone.runs R
    JOIN Mythistone.run_members RM   ON R.run_id     = RM.run_id
    JOIN Mythistone.members M        ON RM.member    = M.member
    JOIN Mythistone.talent_sets ST   ON ST.set_id    = M.talent_set_id AND ST.tree = 1
  WHERE R.`timestamp` > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 14 DAY)) * 1000
  GROUP BY
    M.spec_id, R.season, R.dungeon_id,
    COALESCE(M.hero_talent_id, 0), ST.talent_id;

  RENAME TABLE Mythistone.aggregated_spec_talent     TO Mythistone.aggregated_spec_talent_old,
               Mythistone.aggregated_spec_talent_new TO Mythistone.aggregated_spec_talent;
  DROP TABLE Mythistone.aggregated_spec_talent_old;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_agg_talent_sets_gc`()
BEGIN
  -- Orphan sweep for the talent dictionary. members.talent_set_id has no FK to
  -- talent_sets (an FK would block the season-wipe TRUNCATE), so members that get
  -- purged or wiped leave their dictionary rows behind. Each aggregation cycle
  -- deletes talent_sets rows whose set_id is no longer referenced by any member.
  -- The anti-join uses the index on members.talent_set_id. This is not a shadow
  -- swap; it runs through sp_run_agg_step only for its retry/logging wrapper.
  CALL sp_agg_session_setup();

  DELETE TS FROM Mythistone.talent_sets TS
  WHERE NOT EXISTS (
    SELECT 1 FROM Mythistone.members M
    WHERE M.talent_set_id = TS.set_id
  );
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_capture_lock_holders`(IN p_step VARCHAR(100), IN p_table VARCHAR(128))
BEGIN
  -- Record every session that currently holds a GRANTED metadata lock on
  -- Mythistone.<p_table> (other than us) so a swap that can't get its exclusive
  -- lock leaves a breadcrumb naming the blocker. Best-effort: if performance_schema
  -- MDL instrumentation is disabled, the SELECT simply returns no rows.
  INSERT INTO Mythistone.agg_lock_diag
    (captured_at, step, target_table, holder_processlist_id, holder_user, holder_host,
     holder_command, holder_time, holder_state, holder_info, lock_type, lock_status)
  SELECT
    NOW(), p_step, p_table, t.PROCESSLIST_ID, t.PROCESSLIST_USER, t.PROCESSLIST_HOST,
    t.PROCESSLIST_COMMAND, t.PROCESSLIST_TIME, t.PROCESSLIST_STATE, t.PROCESSLIST_INFO,
    ml.LOCK_TYPE, ml.LOCK_STATUS
  FROM performance_schema.metadata_locks ml
  JOIN performance_schema.threads t ON t.THREAD_ID = ml.OWNER_THREAD_ID
  WHERE ml.OBJECT_SCHEMA = 'Mythistone'
    AND ml.OBJECT_NAME   = p_table
    AND ml.LOCK_STATUS   = 'GRANTED'
    AND t.PROCESSLIST_ID IS NOT NULL
    AND t.PROCESSLIST_ID <> CONNECTION_ID();
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_kill_lock_holders`(IN p_table VARCHAR(128))
BEGIN
  -- Kill only *idle* (Command = 'Sleep') sessions holding a GRANTED metadata lock
  -- on Mythistone.<p_table>. The Sleep guard means we only ever kill a wedged,
  -- idle-in-transaction leaked connection -- never the collector (always actively
  -- writing base tables, never idle-holding a global-table MDL) and never an
  -- actively-running legitimate build.
  DECLARE v_done INT DEFAULT 0;
  DECLARE v_pid  BIGINT DEFAULT NULL;
  DECLARE cur CURSOR FOR
    SELECT DISTINCT t.PROCESSLIST_ID
    FROM performance_schema.metadata_locks ml
    JOIN performance_schema.threads t ON t.THREAD_ID = ml.OWNER_THREAD_ID
    WHERE ml.OBJECT_SCHEMA = 'Mythistone'
      AND ml.OBJECT_NAME   = p_table
      AND ml.LOCK_STATUS   = 'GRANTED'
      AND t.PROCESSLIST_COMMAND = 'Sleep'
      AND t.PROCESSLIST_ID IS NOT NULL
      AND t.PROCESSLIST_ID <> CONNECTION_ID();
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
  -- Ignore "unknown thread id" if the session vanishes between snapshot and KILL.
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;

  OPEN cur;
  kill_loop: LOOP
    FETCH cur INTO v_pid;
    IF v_done THEN
      LEAVE kill_loop;
    END IF;
    SET @kill_sql = CONCAT('KILL ', v_pid);
    PREPARE k FROM @kill_sql;
    EXECUTE k;
    DEALLOCATE PREPARE k;
  END LOOP;
  CLOSE cur;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_migrate_bonus_overnight`()
proc: BEGIN
  DECLARE v_lo    BIGINT;
  DECLARE v_hi    BIGINT;
  DECLARE v_max   BIGINT;
  DECLARE v_step  BIGINT DEFAULT 100000;     -- id-range per batch; tune if needed
  DECLARE v_batch INT    DEFAULT 0;
  DECLARE v_upd   BIGINT DEFAULT 0;
  DECLARE v_total BIGINT DEFAULT 0;
  DECLARE v_err   TEXT;

  -- Log the error and CONTINUE so one hiccup does not kill the night.
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 v_err = MESSAGE_TEXT;
    INSERT INTO Mythistone.bonus_migration_log (ts, phase, detail)
      VALUES (NOW(), 'ERROR', CONCAT('batch=', v_batch, ' lo=', IFNULL(v_lo, -1), ' : ', v_err));
  END;

  SET SESSION group_concat_max_len   = 1000000;   -- a full bonus combo exceeds 1 KB
  SET SESSION innodb_lock_wait_timeout = 60;

  -- Guard against a double-fire.
  IF NOT GET_LOCK('mythi_bonus_migrate', 0) THEN
    INSERT INTO Mythistone.bonus_migration_log (ts, phase, detail)
      VALUES (NOW(), 'SKIP', 'another run already holds the lock');
    LEAVE proc;
  END IF;

  INSERT INTO Mythistone.bonus_migration_log (ts, phase, detail)
    VALUES (NOW(), 'START', CONCAT('step=', v_step, ' backfill starting'));

  SELECT MIN(equipment_id), MAX(equipment_id) INTO v_lo, v_max FROM Mythistone.equipment;

  IF v_lo IS NOT NULL THEN
    WHILE v_lo <= v_max DO
      SET v_hi = v_lo + v_step - 1;

      -- (a) Ensure this range's distinct combos exist in the dictionary.
      --     GROUP_CONCAT is inherently distinct per equipment (PK is
      --     (equipment_id,bonus_id)) and ordered ascending, so the MD5 matches
      --     commonUtils.bonus_set_hash byte-for-byte.
      INSERT IGNORE INTO Mythistone.bonus_sets (set_id, bonus_id)
      SELECT h.set_id, bi.bonus_id
      FROM (
        SELECT b.equipment_id,
               UNHEX(MD5(GROUP_CONCAT(b.bonus_id ORDER BY b.bonus_id SEPARATOR ','))) AS set_id
        FROM Mythistone.bonus_ids b
        WHERE b.equipment_id BETWEEN v_lo AND v_hi
        GROUP BY b.equipment_id
      ) h
      JOIN Mythistone.bonus_ids bi ON bi.equipment_id = h.equipment_id;

      -- (b) Point this range's equipment rows at their set. Only NULLs, so rows
      --     the (new) collector already wrote, and no-bonus rows, are skipped.
      UPDATE Mythistone.equipment e
      JOIN (
        SELECT b.equipment_id,
               UNHEX(MD5(GROUP_CONCAT(b.bonus_id ORDER BY b.bonus_id SEPARATOR ','))) AS set_id
        FROM Mythistone.bonus_ids b
        WHERE b.equipment_id BETWEEN v_lo AND v_hi
        GROUP BY b.equipment_id
      ) h ON e.equipment_id = h.equipment_id
      SET e.bonus_set_id = h.set_id
      WHERE e.bonus_set_id IS NULL;

      SET v_upd   = ROW_COUNT();
      SET v_total = v_total + GREATEST(v_upd, 0);
      SET v_batch = v_batch + 1;

      IF v_batch % 50 = 0 THEN
        INSERT INTO Mythistone.bonus_migration_log (ts, phase, detail)
          VALUES (NOW(), 'PROGRESS',
            CONCAT('batch=', v_batch, ' up_to_id=', v_hi,
                   ' total_updated=', v_total,
                   ' dict_rows=', (SELECT COUNT(*) FROM Mythistone.bonus_sets)));
      END IF;

      SET v_lo = v_hi + 1;
      DO SLEEP(0.1);                          -- breathe between batches
    END WHILE;
  END IF;

  -- Final verdict. still_null = equipment that HAS bonus_ids rows but no pointer;
  -- if that is 0, every bonus-bearing item is migrated and bonus_ids is droppable.
  INSERT INTO Mythistone.bonus_migration_log (ts, phase, detail)
  SELECT NOW(), 'DONE',
    CONCAT('total_updated=', v_total,
           ' dict_rows=', (SELECT COUNT(*) FROM Mythistone.bonus_sets),
           ' equipment_with_bonus_still_null=',
           (SELECT COUNT(*) FROM Mythistone.equipment e
              WHERE e.bonus_set_id IS NULL
                AND EXISTS (SELECT 1 FROM Mythistone.bonus_ids b WHERE b.equipment_id = e.equipment_id)),
           '  <== if that is 0, PART C is safe to run');

  DO RELEASE_LOCK('mythi_bonus_migrate');
END proc;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_refresh_aggregated_embellishments`(IN p_days INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE v_day DATE;
  DECLARE start_sec BIGINT;
  DECLARE end_sec BIGINT;

  SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SESSION LOW_PRIORITY_UPDATES = 1;

  TRUNCATE TABLE Mythistone.aggregated_embellishments;

  WHILE i < p_days DO
    SET v_day = DATE_SUB(CURDATE(), INTERVAL i DAY);
    SET start_sec = UNIX_TIMESTAMP(v_day);
    SET end_sec   = UNIX_TIMESTAMP(DATE_ADD(v_day, INTERVAL 1 DAY)) - 1;

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
        EM.item_id,
        R.run_id,
        EQ.equipment_id
      FROM Mythistone.runs R
      JOIN Mythistone.dungeon_data DD   ON R.dungeon_id = DD.dungeon_id
      JOIN Mythistone.run_members RM    ON R.run_id = RM.run_id
      JOIN Mythistone.members M         ON RM.member = M.member
      JOIN Mythistone.equipment EQ      ON M.member = EQ.member
      JOIN Mythistone.bonus_sets B      ON B.set_id = EQ.bonus_set_id
      JOIN Mythistone.embellishments EM ON EM.bonus_id = B.bonus_id
      WHERE (R.timestamp BETWEEN start_sec AND end_sec)
         OR (R.timestamp BETWEEN start_sec * 1000 AND end_sec * 1000)
      GROUP BY R.run_id, EQ.equipment_id, EM.item_id
    ) t
    GROUP BY
      t.spec_id, t.season, t.dungeon_id, t.keystone_level, t.upgrade_tier,
      t.hero_talent_id, t.item_id
    ON DUPLICATE KEY UPDATE
      run_count = Mythistone.aggregated_embellishments.run_count + VALUES(run_count);

    COMMIT;
    SET i = i + 1;
  END WHILE;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_run_agg_pipeline`()
BEGIN
  CALL sp_agg_session_setup();

  DELETE FROM Mythistone.agg_pipeline_log WHERE started_at < NOW() - INTERVAL 30 DAY;
  DELETE FROM Mythistone.agg_lock_diag    WHERE captured_at < NOW() - INTERVAL 30 DAY;

  -- detail aggregates
  CALL sp_run_agg_step('equipment');
  CALL sp_run_agg_step('enchantments_slot_group');
  CALL sp_run_agg_step('missives');
  CALL sp_run_agg_step('embellishments');
  CALL sp_run_agg_step('crafted_items');
  CALL sp_run_agg_step('loadout_data');
  CALL sp_run_agg_step('bonus_lists');
  CALL sp_run_agg_step('spec');
  CALL sp_run_agg_step('spec_talent');
  CALL sp_run_agg_step('hero_talent');
  IF DAYOFWEEK(CURDATE()) = 3 THEN -- Tuesday, matching the old weekly event's cadence
    CALL sp_run_agg_step('class_talent');
  END IF;
  -- garbage-collect talent dictionary rows no member references any more
  CALL sp_run_agg_step('talent_sets_gc');
  -- garbage-collect bonus dictionary rows no equipment references any more
  CALL sp_run_agg_step('bonus_sets_gc');
  CALL sp_run_agg_step('character_stats');
  CALL sp_run_agg_step('eq_comps');
  CALL sp_run_agg_step('dungeon_specs');
  CALL sp_run_agg_step('dungeon_comps');
  CALL sp_run_agg_step('dungeon_analytics');
  CALL sp_run_agg_step('key_throughput');
  CALL sp_run_agg_step('completion_heatmap');
  CALL sp_run_agg_step('runs_per_dungeon_per_level');

  -- rollups that read the detail aggregates
  CALL sp_run_agg_step('global');
  CALL sp_run_agg_step('global_bonus_lists');
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_run_agg_step`(IN p_step VARCHAR(100))
BEGIN
  DECLARE v_log_id       BIGINT UNSIGNED DEFAULT 0;
  DECLARE v_errno        INT DEFAULT 0;
  DECLARE v_msg          TEXT DEFAULT NULL;
  DECLARE v_err          TEXT DEFAULT NULL;
  DECLARE v_attempt      INT DEFAULT 0;
  DECLARE v_max_attempts INT DEFAULT 5;
  DECLARE v_done         INT DEFAULT 0;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 v_errno = MYSQL_ERRNO, v_msg = MESSAGE_TEXT;
    SET v_err = CONCAT('[', v_errno, '] ', v_msg);
  END;

  INSERT INTO Mythistone.agg_pipeline_log (step, started_at) VALUES (p_step, NOW());
  SET v_log_id = LAST_INSERT_ID();

  SET @agg_call = CONCAT('CALL Mythistone.sp_agg_', p_step, '()');
  PREPARE agg_stmt FROM @agg_call;

  WHILE v_done = 0 AND v_attempt < v_max_attempts DO
    SET v_err   = NULL;
    SET v_errno = 0;
    SET v_attempt = v_attempt + 1;

    EXECUTE agg_stmt;

    IF v_err IS NULL THEN
      SET v_done = 1;                                   -- success
    ELSEIF v_errno = 1205 AND v_attempt < v_max_attempts THEN
      DO SLEEP(30);                                     -- lock wait: back off, retry
    ELSE
      SET v_done = 1;                                   -- non-retryable, or out of attempts
    END IF;
  END WHILE;

  DEALLOCATE PREPARE agg_stmt;

  UPDATE Mythistone.agg_pipeline_log
     SET finished_at = NOW(),
         error = IF(v_err IS NULL AND v_attempt > 1,
                    CONCAT('[ok after ', v_attempt, ' attempts]'),
                    v_err)
   WHERE id = v_log_id;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_season_wipe`()
BEGIN
  DECLARE v_done INT DEFAULT 0;
  DECLARE v_tname VARCHAR(128);
  -- `_new` / `_old` are sp_swap_public_table's shadow tables. They match the
  -- prefixes but are owned by the swap, so leave them alone rather than racing it;
  -- sp_truncate_with_retry tolerates one disappearing anyway.
  DECLARE cur CURSOR FOR
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'Mythistone'
      AND table_type = 'BASE TABLE'
      AND ( table_name LIKE 'aggregated\_%'
         OR table_name LIKE 'global\_aggregated\_%'
         OR table_name LIKE 'simc\_bis\_%'
         OR table_name LIKE 'top\_player\_%' )
      AND table_name NOT LIKE '%\_new'
      AND table_name NOT LIKE '%\_old';
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

  SET FOREIGN_KEY_CHECKS = 0;

  -- raw tables (explicit)
  CALL `Mythistone`.`sp_truncate_with_retry`('runs');
  CALL `Mythistone`.`sp_truncate_with_retry`('run_members');
  CALL `Mythistone`.`sp_truncate_with_retry`('members');
  CALL `Mythistone`.`sp_truncate_with_retry`('equipment');
  CALL `Mythistone`.`sp_truncate_with_retry`('sockets');
  CALL `Mythistone`.`sp_truncate_with_retry`('enchantments');
  CALL `Mythistone`.`sp_truncate_with_retry`('bonus_sets');
  CALL `Mythistone`.`sp_truncate_with_retry`('character_stats');
  CALL `Mythistone`.`sp_truncate_with_retry`('talent_sets');
  CALL `Mythistone`.`sp_truncate_with_retry`('route_data');
  CALL `Mythistone`.`sp_truncate_with_retry`('route_pulls');
  CALL `Mythistone`.`sp_truncate_with_retry`('route_specs');
  CALL `Mythistone`.`sp_truncate_with_retry`('pull_enemies');
  CALL `Mythistone`.`sp_truncate_with_retry`('pull_spells');
  -- trend bar snapshots are period-keyed and season-specific; last season's
  -- weeks are meaningless once the raw data is gone, so clear them too.
  CALL `Mythistone`.`sp_truncate_with_retry`('trend_snapshot');

  -- derived tables (by prefix)
  OPEN cur;
  wipe_loop: LOOP
    FETCH cur INTO v_tname;
    IF v_done = 1 THEN
      LEAVE wipe_loop;
    END IF;
    CALL `Mythistone`.`sp_truncate_with_retry`(v_tname);
  END LOOP wipe_loop;
  CLOSE cur;

  SET FOREIGN_KEY_CHECKS = 1;

  -- reset moving-pointer watermarks so the purge events / top-items rollup
  -- recompute cleanly against the now-empty tables
  UPDATE `Mythistone`.`summary_meta`
    SET last_run_id = 0
  WHERE name IN ('purge_member_pointer', 'purge_routes_pointer', 'aggregated_top_items');
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_swap_public_table`(IN p_base VARCHAR(128))
BEGIN
  -- Atomically swap Mythistone.<p_base>_new into place as Mythistone.<p_base>.
  -- The RENAME/DROP needs an exclusive metadata lock; if a stale reader holds a
  -- shared lock we retry with an escalating lock_wait_timeout, log the holder, and
  -- after a few attempts kill idle holders so the nightly swap still completes.
  DECLARE v_attempt      INT DEFAULT 0;
  DECLARE v_max_attempts INT DEFAULT 5;
  DECLARE v_kill_after   INT DEFAULT 3;
  DECLARE v_done         INT DEFAULT 0;
  DECLARE v_errno        INT DEFAULT 0;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 v_errno = MYSQL_ERRNO;
  END;

  WHILE v_done = 0 AND v_attempt < v_max_attempts DO
    SET v_attempt = v_attempt + 1;
    SET v_errno   = 0;
    SET SESSION lock_wait_timeout = LEAST(60 * v_attempt, 300);

    SET @swap_sql = CONCAT(
      'RENAME TABLE Mythistone.', p_base, ' TO Mythistone.', p_base, '_old, ',
      'Mythistone.', p_base, '_new TO Mythistone.', p_base);
    PREPARE swap_stmt FROM @swap_sql;
    EXECUTE swap_stmt;
    DEALLOCATE PREPARE swap_stmt;

    IF v_errno = 0 THEN
      SET @drop_sql = CONCAT('DROP TABLE IF EXISTS Mythistone.', p_base, '_old');
      PREPARE drop_stmt FROM @drop_sql;
      EXECUTE drop_stmt;
      DEALLOCATE PREPARE drop_stmt;
      SET v_done = 1;                                   -- swap succeeded
    ELSEIF v_errno = 1205 THEN
      CALL sp_capture_lock_holders('swap', p_base);    -- record who is blocking us
      IF v_attempt >= v_kill_after THEN
        CALL sp_kill_lock_holders(p_base);             -- last resort: kill idle holders
      END IF;
      DO SLEEP(5);
    ELSE
      SET v_done = 1;                                   -- non-retryable; leave _new for next run
    END IF;
  END WHILE;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`sp_truncate_with_retry`(IN p_table VARCHAR(128))
BEGIN
  -- TRUNCATE needs an EXCLUSIVE metadata lock, and an MDL wait is governed by
  -- lock_wait_timeout (default 31536000 = one YEAR), not innodb_lock_wait_timeout.
  -- Without an explicit budget, one session sitting idle in a transaction that
  -- touched the table blocks the wipe effectively forever -- while the caller
  -- holds GET_LOCK('agg_pipeline'), which would also stall the nightly pipeline
  -- and the member purge with no error ever raised. So: escalate the budget, log
  -- the blocker, then kill idle holders, exactly as sp_swap_public_table does for
  -- its RENAME. A table that vanished mid-wipe (a shadow table renamed away by a
  -- concurrent swap) is not an error -- there is nothing left to clear.
  DECLARE v_attempt      INT DEFAULT 0;
  DECLARE v_max_attempts INT DEFAULT 4;
  DECLARE v_kill_after   INT DEFAULT 2;
  DECLARE v_done         INT DEFAULT 0;
  DECLARE v_errno        INT DEFAULT 0;

  trunc_scope: BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
      GET DIAGNOSTICS CONDITION 1 v_errno = MYSQL_ERRNO;
    END;

    WHILE v_done = 0 AND v_attempt < v_max_attempts DO
      SET v_attempt = v_attempt + 1;
      SET v_errno   = 0;
      SET SESSION lock_wait_timeout = LEAST(30 * v_attempt, 120);

      SET @trunc_sql = CONCAT('TRUNCATE TABLE `Mythistone`.`', p_table, '`');
      PREPARE trunc_stmt FROM @trunc_sql;
      EXECUTE trunc_stmt;
      DEALLOCATE PREPARE trunc_stmt;

      IF v_errno = 0 THEN
        SET v_done = 1;                                  -- cleared
      ELSEIF v_errno = 1205 THEN
        CALL sp_capture_lock_holders('season_wipe', p_table);
        IF v_attempt >= v_kill_after THEN
          CALL sp_kill_lock_holders(p_table);            -- last resort: idle holders only
        END IF;
        DO SLEEP(5);
      ELSEIF v_errno IN (1146, 1051, 1243) THEN
        SET v_done  = 1;                                 -- table is gone; nothing to clear
        SET v_errno = 0;
      ELSE
        SET v_done = 1;                                  -- non-retryable
      END IF;
    END WHILE;
  END trunc_scope;

  -- Surface an unresolved lock timeout to the caller. The event's EXIT HANDLER
  -- then releases agg_pipeline and leaves request_season raised, so the next tick
  -- retries -- far better than blocking forever holding the shared lock.
  IF v_errno = 1205 THEN
    SET @wipe_err = CONCAT('season wipe: could not get an exclusive lock on ', p_table);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @wipe_err;
  END IF;
END;

CREATE DEFINER=`Test`@`%` PROCEDURE `Mythistone`.`update_aggregated_top_items_proc`()
BEGIN
  -- Get the last processed run_id
  SET @last_run := (SELECT last_run_id FROM summary_meta WHERE name = 'aggregated_top_items');
  SET @new_last_run := (SELECT MAX(run_id) FROM runs);

  -- Only proceed if there are new runs
  IF @new_last_run > @last_run THEN

    -- Insert or update aggregated data for new runs only
    INSERT INTO aggregated_top_items (spec_id, hero_talent_id, slot, item_id, bonus_combo, run_count, season)
    SELECT
      m.spec_id,
      m.hero_talent_id,
      e.slot,
      e.item_id,
      GROUP_CONCAT(DISTINCT b.bonus_id ORDER BY b.bonus_id ASC SEPARATOR ':') AS bonus_combo,
      COUNT(*) AS run_count,
      r.season
    FROM equipment e
    JOIN members m ON e.member = m.member
    JOIN run_members rm ON m.member = rm.member
    JOIN runs r ON rm.run_id = r.run_id
    LEFT JOIN bonus_sets b ON b.set_id = e.bonus_set_id
    WHERE r.run_id > @last_run AND r.run_id <= @new_last_run
    GROUP BY
      m.spec_id,
      m.hero_talent_id,
      e.slot,
      e.item_id,
      bonus_combo,
      r.season
    ON DUPLICATE KEY UPDATE
      run_count = run_count + VALUES(run_count);

    -- Update the last processed run_id
    UPDATE summary_meta SET last_run_id = @new_last_run WHERE name = 'aggregated_top_items';
  END IF;
END;

CREATE EVENT ev_nightly_agg_pipeline
ON SCHEDULE EVERY 1 DAY
STARTS '2026-07-11 20:00:00.000'
ON COMPLETION PRESERVE
DISABLE
COMMENT 'Runs all nightly aggregations sequentially; per-step log in agg_pipeline_log'
DO BEGIN
  IF GET_LOCK('agg_pipeline', 0) = 1 THEN
    CALL Mythistone.sp_run_agg_pipeline();
    DO RELEASE_LOCK('agg_pipeline');
  END IF;
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

  -- Stand down while the nightly pipeline / member purge / season wipe holds the
  -- shared lock, exactly like ev_purge_old_run_details_incremental does. Without
  -- this, a season wipe's TRUNCATE route_data races this DELETE for the same
  -- table's metadata lock and needlessly stalls the wipe.
  IF IS_USED_LOCK('agg_pipeline') IS NOT NULL THEN
    LEAVE purge_block;
  END IF;

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
COMMENT 'Moving-pointer purge of member details: purges members with no run in the last 14 days that is also within 5 keys of the dungeon current max — yields to collectors and the nightly pipeline'
DO purge_block: BEGIN
  DECLARE v_cutoff_ts    BIGINT DEFAULT 0;
  DECLARE v_ptr          BIGINT DEFAULT 0;
  DECLARE v_process_up_to BIGINT DEFAULT 0;
  DECLARE v_member_window BIGINT DEFAULT 10000;
  DECLARE v_found        INT DEFAULT 0;
  DECLARE v_max_member   BIGINT DEFAULT 0;
  DECLARE v_max_season   INT DEFAULT 0;
  DECLARE v_key_margin   INT DEFAULT 5;   -- runs this many keys below the dungeon current max no longer keep a member alive
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    -- pointer not advanced: the next tick simply redoes this window
    DO RELEASE_LOCK('purge_members_lock');
  END;

  -- don't fight the nightly aggregation pipeline
  IF IS_USED_LOCK('agg_pipeline') IS NOT NULL THEN
    LEAVE purge_block;
  END IF;

  -- GET_LOCK returns 1 if acquired, 0 if another instance holds it.
  IF GET_LOCK('purge_members_lock', 0) = 0 THEN
    LEAVE purge_block;   -- a prior invocation is still running; skip this cycle
  END IF;

  SET SESSION innodb_lock_wait_timeout = 15;  -- yield to collectors
  SET SESSION lock_wait_timeout = 60;

  -- 1. Compute cutoff (14 days ago in ms)
  SET v_cutoff_ts = (UNIX_TIMESTAMP() * 1000) - 14 * 24 * 3600 * 1000;

  -- current season only (old seasons are wiped; MAX(season) matches how the
  -- rest of this file scopes current-season data)
  SELECT MAX(season) INTO v_max_season FROM Mythistone.runs;

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

  -- 4. MEMORY temp table — 10K INT rows, stays in RAM, zero ibtmp1 cost
  DROP TEMPORARY TABLE IF EXISTS tmp_purge_members;
  CREATE TEMPORARY TABLE tmp_purge_members (
    member INT UNSIGNED PRIMARY KEY
  ) ENGINE=MEMORY;

  -- 4b. Current max key per dungeon, built once per tick (~8 dungeons) and
  --     joined per member below — cheaper than recomputing it per member.
  --     Scoped to the current season; global across regions (owner asked for
  --     the "current max key for a dungeon" with no region qualifier). Column
  --     charset/collation matches runs.dungeon_id so the join below never hits
  --     an illegal-mix-of-collations error.
  DROP TEMPORARY TABLE IF EXISTS tmp_dungeon_max;
  CREATE TEMPORARY TABLE tmp_dungeon_max (
    dungeon_id VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY,
    max_level  INT UNSIGNED NOT NULL
  ) ENGINE=MEMORY;

  INSERT INTO tmp_dungeon_max (dungeon_id, max_level)
  SELECT r.dungeon_id, MAX(r.keystone_level)
  FROM Mythistone.runs r
  WHERE r.season = v_max_season
  GROUP BY r.dungeon_id;

  -- 5. Find purgeable members in window. A run keeps a member alive only if it
  --    is BOTH recent (newer than the 14-day cutoff) AND high-key (within
  --    v_key_margin of the dungeon's current max). SUM(...) counts keep-alive
  --    runs per member; = 0 means none exist, so the member is purged. The
  --    14-day cutoff stays the outer bound: an old run never keeps a member
  --    alive regardless of key. LEFT JOIN + COALESCE errs toward keeping data:
  --    a run whose dungeon has no current-season max (should not happen post
  --    season-wipe) is treated as high-key rather than over-purged.
  INSERT INTO tmp_purge_members (member)
  SELECT rm.member
  FROM Mythistone.run_members rm
  JOIN Mythistone.runs r ON rm.run_id = r.run_id
  LEFT JOIN tmp_dungeon_max dm ON dm.dungeon_id = r.dungeon_id
  WHERE rm.member BETWEEN (v_ptr + 1) AND v_process_up_to
  GROUP BY rm.member
  HAVING SUM(
           r.timestamp > v_cutoff_ts
           AND r.keystone_level >= CAST(COALESCE(dm.max_level, 0) AS SIGNED) - v_key_margin
         ) = 0;

  SELECT COUNT(*) INTO v_found FROM tmp_purge_members;

  -- 6. Delete — one auto-committed statement per table, so lock scope stays
  --    small and a failure loses nothing (idempotent, pointer not yet moved)
  IF v_found > 0 THEN
    -- Talent rows now live in the talent_sets dictionary keyed by
    -- members.talent_set_id. The purge keeps the member row (run_members / comps
    -- still reference it), so NULL its dictionary pointer instead: that drops the
    -- member out of the talent aggregations' INNER JOIN exactly as deleting its
    -- old per-member class/spec/hero_talents rows used to, and lets the
    -- aggregation cycle's sp_agg_talent_sets_gc orphan sweep reclaim any set no
    -- surviving member references.
    UPDATE Mythistone.members M
      INNER JOIN tmp_purge_members tmp ON M.member = tmp.member
      SET M.talent_set_id = NULL;
    DELETE eq FROM Mythistone.equipment eq
      INNER JOIN tmp_purge_members tmp ON eq.member = tmp.member;
    DELETE cs FROM Mythistone.character_stats cs
      INNER JOIN tmp_purge_members tmp ON cs.member = tmp.member;
  END IF;

  UPDATE Mythistone.summary_meta
    SET last_run_id = v_process_up_to
  WHERE name = 'purge_member_pointer';

  -- 7. Cleanup
  DROP TEMPORARY TABLE IF EXISTS tmp_purge_members;
  DROP TEMPORARY TABLE IF EXISTS tmp_dungeon_max;

  DO RELEASE_LOCK('purge_members_lock');

END purge_block;

CREATE EVENT ev_season_wipe
ON SCHEDULE EVERY 10 MINUTE
STARTS '2026-08-07 14:10:04.000'
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Blanket season-rollover clear; runs only when CI raised a request and the collector has paused'
DO wipe_block: BEGIN
  DECLARE v_req    INT DEFAULT 0;
  DECLARE v_done   INT DEFAULT 0;
  DECLARE v_paused TINYINT DEFAULT 0;
  DECLARE v_log_id BIGINT UNSIGNED DEFAULT NULL;
  DECLARE v_msg    TEXT DEFAULT NULL;
  -- Never leak the shared lock if the clear errors; the request stays raised so a
  -- later tick retries (idempotent). Record why it failed too — a silently
  -- abandoned wipe is otherwise invisible until someone notices the DB never
  -- shrank. Order matters: read the diagnostics area first (anything else
  -- overwrites it), release the lock next (so a failing log write can't strand
  -- it), and only then write the log row.
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 v_msg = MESSAGE_TEXT;
    DO RELEASE_LOCK('agg_pipeline');
    IF v_log_id IS NOT NULL THEN
      UPDATE `Mythistone`.`agg_pipeline_log`
        SET finished_at = NOW(),
            error = COALESCE(v_msg, 'season wipe failed')
      WHERE id = v_log_id;
    END IF;
  END;

  SELECT request_season, done_season, collector_paused
    INTO v_req, v_done, v_paused
  FROM `Mythistone`.`wipe_control`
  WHERE id = 1;

  IF v_req <= v_done THEN
    LEAVE wipe_block;                                   -- nothing requested
  END IF;
  IF v_paused = 0 THEN
    LEAVE wipe_block;                                   -- wait for the collector to ack the pause
  END IF;
  IF IS_USED_LOCK('agg_pipeline') IS NOT NULL THEN
    LEAVE wipe_block;                                   -- nightly pipeline / purge is running
  END IF;
  IF GET_LOCK('agg_pipeline', 0) = 0 THEN
    LEAVE wipe_block;                                   -- couldn't grab the lock this tick
  END IF;

  SET SESSION innodb_lock_wait_timeout = 15;            -- row locks: yield rather than block
  -- Metadata locks are a SEPARATE budget, and its default is one year. TRUNCATE
  -- takes an exclusive MDL, so without this a single idle-in-transaction session
  -- would hang this event forever while it holds agg_pipeline.
  SET SESSION lock_wait_timeout = 30;

  INSERT INTO `Mythistone`.`agg_pipeline_log` (step, started_at)
  VALUES (CONCAT('season_wipe:', v_req), NOW());
  SET v_log_id = LAST_INSERT_ID();

  CALL `Mythistone`.`sp_season_wipe`();

  UPDATE `Mythistone`.`wipe_control`
    SET done_season = v_req,
        request_season = 0
  WHERE id = 1;

  UPDATE `Mythistone`.`agg_pipeline_log`
    SET finished_at = NOW()
  WHERE id = v_log_id;

  DO RELEASE_LOCK('agg_pipeline');
END wipe_block;