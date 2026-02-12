-- ===========================
-- SQL Server versie van JPL Data
-- ===========================

-- 1. Drop database als deze bestaat
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'JPL_Data')
BEGIN
    ALTER DATABASE JPL_Data SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE JPL_Data;
END
GO

-- 2. Maak de database aan
CREATE DATABASE JPL_Data;
GO

-- 3. Gebruik de database
USE JPL_Data;
GO

-- ===========================
-- 4. Tabellen aanmaken
-- ===========================

-- Team tabel
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Team') AND type in (N'U'))
BEGIN
    CREATE TABLE Team (
        team_naam NVARCHAR(100) PRIMARY KEY,
        latitude DECIMAL(9,6) NULL,
        longitude DECIMAL(9,6) NULL,
        stadion_capaciteit INT NULL
    );
END
GO

-- Wedstrijd tabel
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Wedstrijd') AND type in (N'U'))
BEGIN
    CREATE TABLE Wedstrijd (
        wedstrijd_id INT IDENTITY(1,1) PRIMARY KEY,
        divisie NVARCHAR(50) NOT NULL,
        wedstrijd_datum DATE NOT NULL,
        wedstrijd_tijd TIME NOT NULL,
        thuis_team NVARCHAR(100) NOT NULL,
        uit_team NVARCHAR(100) NOT NULL,

        CONSTRAINT FK_Wedstrijd_ThuisTeam FOREIGN KEY (thuis_team) REFERENCES Team(team_naam),
        CONSTRAINT FK_Wedstrijd_UitTeam FOREIGN KEY (uit_team) REFERENCES Team(team_naam)
    );
END
GO

-- WedstrijdStatistiek tabel
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'WedstrijdStatistiek') AND type in (N'U'))
BEGIN
    CREATE TABLE WedstrijdStatistiek (
        wedstrijd_id INT PRIMARY KEY,

        doelpunten_thuis_voltijd INT NULL,
        doelpunten_uit_voltijd INT NULL,
        resultaat_voltijd CHAR(1) NULL,

        doelpunten_thuis_halftijd INT NULL,
        doelpunten_uit_halftijd INT NULL,
        resultaat_halftijd CHAR(1) NULL,

        schoten_thuis INT NULL,
        schoten_uit INT NULL,
        schoten_op_doel_thuis INT NULL,
        schoten_op_doel_uit INT NULL,

        overtredingen_thuis INT NULL,
        overtredingen_uit INT NULL,

        corners_thuis INT NULL,
        corners_uit INT NULL,

        gele_kaarten_thuis INT NULL,
        gele_kaarten_uit INT NULL,
        rode_kaarten_thuis INT NULL,
        rode_kaarten_uit INT NULL,

        CONSTRAINT FK_WedstrijdStatistiek_Wedstrijd FOREIGN KEY (wedstrijd_id) REFERENCES Wedstrijd(wedstrijd_id)
    );
END
GO

-- WedstrijdWeer tabel
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'WedstrijdWeer') AND type in (N'U'))
BEGIN
    CREATE TABLE WedstrijdWeer (
        wedstrijd_id INT PRIMARY KEY,

        temperatuur_c DECIMAL(4,1) NULL,
        neerslag_mm DECIMAL(5,2) NULL,
        relatieve_luchtvochtigheid_pct INT NULL,

        windsnelheid_m_s DECIMAL(4,1) NULL,
        windrichting_graden INT NULL,
        windstoten_m_s DECIMAL(4,1) NULL,

        bewolking_pct INT NULL,
        weercode INT NULL,
        luchtdruk_hpa DECIMAL(6,1) NULL,

        temperatuur_gem_c DECIMAL(4,1) NULL,
        neerslag_som_mm DECIMAL(5,2) NULL,
        windsnelheid_gem_m_s DECIMAL(4,1) NULL,
        windstoten_max_m_s DECIMAL(4,1) NULL,

        CONSTRAINT FK_WedstrijdWeer_Wedstrijd FOREIGN KEY (wedstrijd_id) REFERENCES Wedstrijd(wedstrijd_id)
    );
END
GO

-- WedstrijdWeerVooraf tabel
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'WedstrijdWeerVooraf') AND type in (N'U'))
BEGIN
    CREATE TABLE WedstrijdWeerVooraf (
        wedstrijd_id INT PRIMARY KEY,

        temperatuur_gem_laatste48_c DECIMAL(4,1) NULL,
        neerslag_som_laatste48_mm DECIMAL(6,2) NULL,
        windstoten_max_laatste48_m_s DECIMAL(4,1) NULL,
        regen_uren_laatste48 INT NULL,
        hitte_uren_laatste48 INT NULL,

        CONSTRAINT FK_WedstrijdWeerVooraf_Wedstrijd FOREIGN KEY (wedstrijd_id) REFERENCES Wedstrijd(wedstrijd_id)
    );
END
GO

-- ===========================
-- 5. Indexen
-- ===========================

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'idx_wedstrijd_thuis_team' AND object_id = OBJECT_ID(N'Wedstrijd'))
BEGIN
    CREATE INDEX idx_wedstrijd_thuis_team ON Wedstrijd(thuis_team);
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'idx_wedstrijd_uit_team' AND object_id = OBJECT_ID(N'Wedstrijd'))
BEGIN
    CREATE INDEX idx_wedstrijd_uit_team ON Wedstrijd(uit_team);
END
GO
