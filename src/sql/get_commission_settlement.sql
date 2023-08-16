SELECT [CashFlowDate]
      ,[EirExposureMapId]
      ,[EirCommissionExternalId]
      ,[AmortizationResultsLinear]
      ,[AmortizationResultsEffective]
      ,[AmortizationCumulatedLinear]
      ,[AmortizationCumulatedEffective]
      ,[UnsettledCommissionBalance]
      ,[CommissionRatio]
      ,[CommissionMethod]
      ,[CommValue]
      ,[CommType]
      ,[BusinessDate]
      ,[CommissionSign]
      ,[CollectionDate]
      ,[CommissionSettlementDate]
FROM [Eir.Calc].[EirCommissionSettlement] S
JOIN [Eir.Data].[EirDataset] D
ON S.EirDatasetTaskExecutionId = D.TaskExecutionId
WHERE S.EirDatasetTaskExecutionId = ?
      AND EirCalculationTaskExecutionId = (
            SELECT PreviousEirCalculationTaskExecutionId
            FROM [Eir.Calc].EirCalculation
            WHERE TaskExecutionId = ?
      ) AND {where}
ORDER BY BusinessDate ASC