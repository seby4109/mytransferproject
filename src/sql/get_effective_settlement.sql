SELECT [CashFlowDate]
      ,[EirExposureMapId]
      ,[NominalInterestAccrual]
      ,[EffectiveInterestAccrual]
      ,[AmortizationResultsEffective]
      ,[AmortizationCumulatedEffective]
      ,[UnsettledCommissionBalanceEffective]
      ,[BusinessDate]
FROM [Eir.Calc].[EirEffectiveSettlement] S
JOIN [Eir.Data].[EirDataset] D
ON S.EirDatasetTaskExecutionId = D.TaskExecutionId
WHERE S.EirDatasetTaskExecutionId = ? 
      AND EirCalculationTaskExecutionId = (
            SELECT PreviousEirCalculationTaskExecutionId
            FROM [Eir.Calc].EirCalculation
            WHERE TaskExecutionId = ?
      ) AND {where}
ORDER BY BusinessDate ASC