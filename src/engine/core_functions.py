from numba import njit

class Core:
    '''
    xirr - static, takes 2 numpy arrays dates, cashflows, returns irr, 
    uses private method _xirr_numba 
    '''
    @staticmethod
    @njit
    def _xirr_numba(years, cashflows):
        residual = 1
        step = 0.05
        guess = 0.1
        epsilon = 0.0000000001 # TODO: jak to dobrze ustawic, zeby za dlugo nie liczylo, ale bylo 
        limit = 100000
        while abs(residual) > epsilon and limit > 0:
            limit -= 1
            residual = 0.0
            for i, cf in enumerate(cashflows):
                residual += cf / pow(guess, years[i])
            if abs(residual) > epsilon:
                if residual > 0:
                    guess += step
                else:
                    guess -= step
                    step /= 2.0
        return guess-1
    
    @staticmethod
    def xirr(dates, cashflows):
        years = (dates - dates[0]).astype('float64')/365.0
        return Core._xirr_numba(years, cashflows)
