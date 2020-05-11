USE [MetricsDyetl];
GO
SET ANSI_NULLS ON
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

//****** Object: Table [dbo].[VendorRisk] Script Date: 05 11 2020 09:52:05 AM ******//

CREATE TABLE [dbo].[VendorRisk]
([Engagement Name] varchar(max) NULL
[Relationship Name] varchar(max) NULL
[Engagement Number] varchar(max) NULL
[Relationship Number] varchar(max) NULL
[Contract End Date] None NULL
[Next Ongoing Monitoring Launch Date] None NULL
[Business] varchar(max) NULL
[Inherent Risk Rating] varchar(max) NULL
[Residual Risk Rating] varchar(max) NULL
[Sourcing Professional] varchar(max) NULL
[Engagement Manager] varchar(max) NULL
[Project Classification] varchar(max) NULL
[Description] varchar(max) NULL
[Due Diligence Status] varchar(max) NULL
[Candidate Status] varchar(max) NULL
[Status] varchar(max) NULL
[Vendor] varchar(max) NULL,
) ON [PRIMARY];

GO
