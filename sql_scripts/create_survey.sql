CREATE TABLE IF NOT EXISTS survey (
    `EIN` FLOAT (48) NOT NULL,
    `ADDRESS` VARCHAR (255),
    `CITY` VARCHAR (255),
    `STATE` VARCHAR (255),
    `ZIP` VARCHAR (255),
    `ORG` VARCHAR (255),
    `LATITUDE` FLOAT (48),
    `LONGITUDE` FLOAT (48),
    PRIMARY KEY (`EIN`)
);