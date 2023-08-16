WITH linking_tasks AS (
      SELECT TaskExecutionId, Name, PreviousEirCalculationTaskExecutionId
      FROM [Eir.Calc].EirCalculation
      WHERE TaskExecutionId = ?
      UNION ALL
      SELECT B.TaskExecutionId, B.Name, B.PreviousEirCalculationTaskExecutionId
      FROM [Eir.Calc].EirCalculation B
      JOIN linking_tasks L
      ON B.TaskExecutionId = L.PreviousEirCalculationTaskExecutionId
)
SELECT [EirCommissionExternalId]
      ,Comm.[EirExposureMapId]
      ,[CollectionDate]
      ,[CommissionSettlementDate]
      ,[CommValue]
      ,[CommType]
      ,[BusinessDate]
	  ,E.[ProductType]
	  ,E.[ProductSubType]
	  ,Conf.[CommissionMethodType]
	  ,Conf.[SettlementType]
        ,Conf.[CommissionSignType]
FROM [Eir.Data].[EirCommission] Comm
JOIN [Eir.Data].[EirExposure] E
ON (Comm.EirExposureMapId = E.EirExposureMapId AND Comm.EirDatasetTaskExecutionId = E.EirDatasetTaskExecutionId)
JOIN [Eir.Data].[EirDataset] D
ON Comm.EirDatasetTaskExecutionId = D.TaskExecutionId
LEFT JOIN (
	SELECT [Id]
      ,[ProductType]
      ,[ProductSubType]
      ,[CommissionType]
      ,[SettlementType]
      ,[CommissionSignType]
      ,[CommissionMethodType]
      ,[EirConfigurationTaskExecutionId]
  FROM [Eir.Conf].[EirConfigurationItem]
  WHERE [EirConfigurationTaskExecutionId] = ?
) Conf
ON (Comm.CommType = Conf.CommissionType
	AND E.ProductType = Conf.ProductType
	AND E.ProductSubType = Conf.ProductSubType
)
WHERE Comm.EirDatasetTaskExecutionId IN(
      SELECT EirDatasetTaskExecutionId FROM [Eir.Calc].EirCalculation
      WHERE TaskExecutionId IN (
            SELECT TaskExecutionId
            FROM linking_tasks)
      ) AND {where}
ORDER BY [BusinessDate] DESC