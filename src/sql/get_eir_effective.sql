SELECT [BusinessDate]
      ,[EirExposureMapId]
      ,[CashFlowDate]
      ,[NotionalDue]
      ,[RepaymentCapital]
      ,[RepaymentInterest]
      ,[RepaymentAccruedInterest]
      ,[CommValue]
      ,[AmortizationResultsEffective]
      ,[UnsettledCommissionBalanceEffective]
      ,[EffectiveInterest]
      ,[EffectiveNotionalDue]
      ,[EffectiveInterestRate]
FROM [Eir.Calc].[EirEffectiveAmortization] A
JOIN [Eir.Data].[EirDataset] D
ON A.EirDatasetTaskExecutionId = D.TaskExecutionId
WHERE A.EirDatasetTaskExecutionId = ?
      AND EirCalculationTaskExecutionId = (
            SELECT PreviousEirCalculationTaskExecutionId
            FROM [Eir.Calc].EirCalculation
            WHERE TaskExecutionId = ?
      ) AND {where}
ORDER BY [BusinessDate] ASC, [CashFlowDate] ASC