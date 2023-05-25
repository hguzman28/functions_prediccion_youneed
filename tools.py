from numpy import zeros

class PredictTools:
    
    @staticmethod
    def intervalo_1(df):
        intervalo1 = zeros(len(df))
        for i in range(len(df)):
            if i == 0:
                aux = df[0:4]['maximo_predict'].sum()
                if aux >= 1:
                    intervalo1[i] = 1
            elif i == 1:
                aux = df[0:2]['maximo_predict'].sum()+df[1:5]['maximo_predict'].sum()
                if aux >= 1:
                    intervalo1[i]  = 1
            elif i == 2:
                aux = df[0:3]['maximo_predict'].sum()+df[2:6]['maximo_predict'].sum()
                if aux >= 1:
                    intervalo1[i]= 1
            elif i == len(df)-2:
                aux = df[i-4:i]['maximo_predict'].sum()+df[i:i+3]['maximo_predict'].sum()
                if aux >= 1:
                    intervalo1[i] = 1
            elif i == len(df)-1:
                aux = df[i-4:i+1]['maximo_predict'].sum()+df[i:i+2]['maximo_predict'].sum()
                if aux >= 1:
                    intervalo1[i] = 1
            elif i == len(df):
                aux = df[i-4:i]['maximo_predict'].sum()
                if aux >= 1:
                    intervalo1[i] = 1
            else:
                aux = df[i-3:i+1]['maximo_predict'].sum()+df[i:i+4]['maximo_predict'].sum()
                if aux >= 1:
                    intervalo1[i] =1
        return intervalo1
    
    