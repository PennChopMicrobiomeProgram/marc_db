/*
 * Schema definition for SQLite3 mARC database
 */

CREATE TABLE IF NOT EXISTS `isolates` (
  `id` INTEGER PRIMARY KEY AUTOINCREMENT,
  `subject_id` INTEGER NOT NULL,
  `specimen_id` INTEGER NOT NULL,
  `source` TEXT,
  `suspected_organism` TEXT DEFAULT 'unknown',
  `special_collection` TEXT,
  `received_date` DATE,
  `cryobanking_date` DATE,
);

CREATE TABLE IF NOT EXISTS `aliquots` (
  `id` INTEGER PRIMARY KEY AUTOINCREMENT,
  `isolate_id` INTEGER NOT NULL,
  `tube_barcode` TEXT NOT NULL,
  `box_name` TEXT NOT NULL,
  FOREIGN KEY (`isolate_id`) REFERENCES `isolates`(`id`)
);