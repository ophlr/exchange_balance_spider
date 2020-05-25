CREATE TABLE IF NOT EXISTS `exchange_eth_address` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `address` varchar(100) COLLATE utf8mb4_general_ci NOT NULL,
  `exchange` varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
  `tag` varchar(100) COLLATE utf8mb4_general_ci NOT NULL,
  `status` varchar(10) COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'active',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `address` (`address`),
  KEY exchange_address(`exchange`, `address`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `exchange_balance` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `address` varchar(100) COLLATE utf8mb4_general_ci NOT NULL,
  `balance` decimal(60,10) NOT NULL,
  `currency` varchar(20) COLLATE utf8mb4_general_ci NOT NULL,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY address_currency_time (`address`, `currency`, `create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `exchange_btc_balances` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `exchange` varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
  `balance` decimal(60,10) NOT NULL,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY exchange_time (`exchange`, `create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `exchange_btc_address` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `address` varchar(100) COLLATE utf8mb4_general_ci NOT NULL,
  `exchange` varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
  `tag` varchar(100) COLLATE utf8mb4_general_ci,
  `status` varchar(10) COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'active',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `address` (`address`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


CREATE TABLE IF NOT EXISTS `exchange_eth_balances` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `exchange` varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
  `balance` decimal(60,10) NOT NULL,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY exchange_time (`exchange`, `create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


CREATE TABLE IF NOT EXISTS `transparent_agency` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `agency` varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
  `intro_en` varchar(512) COLLATE utf8mb4_general_ci NOT NULL,
  `intro_cn` varchar(512) COLLATE utf8mb4_general_ci NOT NULL,
  `intro_tw` varchar(512) COLLATE utf8mb4_general_ci NOT NULL,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY agency_time (`agency`, `create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


CREATE TABLE IF NOT EXISTS `exchange_chain_address` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `exchange` varchar(50) NOT NULL,
    `chain` varchar(50) NOT NULL,
    `address` varchar(200) NOT NULL,
    `tag` varchar(1024) NULL,
    `source` varchar(1024) NULL,
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `address_inx` (`address`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `balance_of_address_history` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `address` varchar(200) NOT NULL,
    `coin` varchar(50) NOT NULL,
    `balance` varchar(100) NOT NULL,
    `source` varchar(1024) NULL,
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `balance_of_exchange_history` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `exchange` varchar(50) NOT NULL,
    `chain` varchar(50) NOT NULL,
    `coin` varchar(200) NOT NULL,
    `balance` varchar(100) NOT NULL,
    `source` varchar(1024) NULL,
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


insert into balance_of_address_history(address, coin, balance, source, create_time) select address, currency, balance, "old", create_time from exchange_balance;

insert into exchange_chain_address(exchange, chain, address, tag, source, create_time, modify_time) select exchange, "BTC", address, tag, "old", create_time, modify_time from exchange_btc_address;
insert into exchange_chain_address(exchange, chain, address, tag, source, create_time, modify_time) select exchange, "ETH", address, tag, "old", create_time, modify_time from exchange_eth_address;

insert into balance_of_exchange_history(exchange, chain, coin, balance, source, create_time) select exchange, "multiple", "BTC", balance, "old", create_time from exchange_btc_balances;
insert into balance_of_exchange_history(exchange, chain, coin, balance, source, create_time) select exchange, "multiple", "ETH", balance, "old", create_time from exchange_eth_balances;

