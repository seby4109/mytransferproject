SELECT [Date]
      ,[EirExposureMapId]
      ,[Capital]
      ,[Interest]
	,[BusinessDate]
FROM [Eir.Data].[EirPaymentSchedule] P
JOIN [Eir.Data].[EirDataset] D
ON P.EirDatasetTaskExecutionId = D.TaskExecutionId
WHERE P.EirDatasetTaskExecutionId IN (?, ?) AND {where}
ORDER BY [BusinessDate] ASC, [Date] ASC
