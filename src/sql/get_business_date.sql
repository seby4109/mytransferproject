SELECT BusinessDate
FROM [Eir.Data].EirDataset
WHERE TaskExecutionId = (
    SELECT [EirDatasetTaskExecutionId]
    FROM [Eir.Calc].[EirCalculation]
    {where}
)