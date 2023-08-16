SELECT [EirExposureMapId]
      ,[CptyIdentifier]
      ,[LimitIdentifier]
      ,[LimitAmount]
      ,[ExpCcy]
      ,[ProductType]
      ,[ProductSubType]
      ,[OpenValue]
      ,[Notional]
      ,[AccruedInterest]
      ,[Nir]
      ,[RateBasis]
      ,[InterestRateMargin]
      ,[RateBasisType]
      ,[RateAlgorithmType]
      ,[IsContractModification]
      ,[OpenDate]
      ,[MaturityDate]
      ,[AccrualConventionType]
      ,[BusinessDate]
FROM [Eir.Data].[EirExposure] E
JOIN [Eir.Data].[EirDataset] D
ON E.EirDatasetTaskExecutionId = D.TaskExecutionId
WHERE E.EirDatasetTaskExecutionId IN (?, ?) AND {where}
ORDER BY BusinessDate ASC