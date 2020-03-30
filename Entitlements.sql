USE [MetricsDyetl];
GO
SET ANSI_NULLS ON
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

//****** Object: Table [dbo].[Entitlements] Script Date: 03 30 2020 10:20:54 AM ******//

CREATE TABLE [dbo].[Entitlements]
([UserName] varchar(max) NOT NULL
[Path] varchar(max) NOT NULL
[Name] varchar(max) NOT NULL
[RoleName] varchar(max) NOT NULL
[Description] varchar(max) NOT NULL
[ServerName] varchar(max) NOT NULL
[ServerType] varchar(max) NOT NULL
[CreateDate] varchar(max) NOT NULL,
) ON [PRIMARY];

GO
