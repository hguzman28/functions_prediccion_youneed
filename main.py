from tools import PredictTools
from pandas import DataFrame, DatetimeIndex
from numpy import where, zeros


class Predict:
    
    ptools = PredictTools()
    
    @staticmethod
    def predic(model,x_test):
        return  model.predict(x_test) 
    
    

    def fun_dataset_prediccion(self,ypred, x_test, escalador, nombres_datos_test, datos_testeo, fecha_test):
        """_summary_

        Args:
            ypred (_type_): Demanda predicha (demanda_p)
            x_test (_type_): base_forecasting
            escalador (_type_): _description_
            nombres_datos_test (_type_): Nombres de las columnas de base_forecasting
            datos_testeo (_type_): concat base forecasting y base modelo y luego hacer esta operacion df[(df.fechahora >= fecha_inicio_test)
                                    & (df.fechahora <= fecha_final_test)] las fechas son las de prediccion
            fecha_test (_type_): datos_test[['fechahora']].values


        Returns:
            _type_: _description_
        """
        
        nombres_datos_test = list(nombres_datos_test)
        nombres_datos_test.insert(0, 'demanda')
        #nombres_datos_test.append('demanda')

        df_yhat = x_test.copy()
        #fechahora = df_yhat.fechahora
        df_yhat['demanda'] = ypred
        df_yhat['demanda'] = df_yhat['demanda'].fillna(0)
        df_yhat = df_yhat[nombres_datos_test]
        
        df_yhat_inverse_values = escalador.inverse_transform(df_yhat)
        df_yhat_inverse = DataFrame(df_yhat_inverse_values)
        df_yhat_inverse.columns = nombres_datos_test
        #df_yhat_inverse['fechahora'] = fechahora
        datos_testeo = datos_testeo.drop(['fechahora'], axis=1)
        datos_test_values = escalador.inverse_transform(datos_testeo)
        datos_test = DataFrame(datos_test_values)
        datos_test.columns =  nombres_datos_test
        datos_test['fechahora'] = fecha_test
        #df_predict = datos_test.copy()

        #var_df_lag.append('fechahora')
        #print(var_df_lag)
        #df_predict_ = df_predict.merge(df_lags[var_df_lag], on = 'fechahora', how='left')
        datos_test['Prediccion'] = df_yhat_inverse[['demanda']]
        #df_predict['fecha_'], df_predict['hour']= df_predict.fechahora.str.split(expand= True)[0],df_predict.fechahora.str.split(expand= True)[1]
        datos_test['year'],datos_test['month'],datos_test['day'], datos_test['hour'] = DatetimeIndex(datos_test.fechahora).year,  DatetimeIndex(datos_test.fechahora).month, DatetimeIndex(datos_test.fechahora).day, DatetimeIndex(datos_test.fechahora).hour
        datos_test['Horapunta'] = where(datos_test.hour.isin(["18:00","18:15","18:30","18:45","19:00","19:15","19:30","19:45","20:00","20:15","20:30","20:45","21:00"]),1,0)
        max_prediccion = datos_test[datos_test['Horapunta']== 1].groupby(['year','month','day'],as_index= False)['Prediccion'].max()
        max_prediccion['maximo_predict'] = 1
        base_prediccion = datos_test.merge(max_prediccion, how = 'left', left_on=['year', 'month', 'day', 'Prediccion'],right_on=['year', 'month', 'day', 'Prediccion'])
        base_prediccion['maximo_predict'] = base_prediccion['maximo_predict'].fillna(0)

        base_prediccion['intervalo1'] = self.ptools.intervalo_1(base_prediccion)
        
        base_prediccion = base_prediccion.drop(['demanda'], axis=1)
        # base_prediccion = base_prediccion[['fechahora','demanda','intervalo1']]
        
        return base_prediccion
    
    @staticmethod
    def final_prediction(model, x_test):
        
        demanda_p = zeros(x_test.shape[0])
        x_test = x_test.set_index('fechahora')
            
        for i in range(len(demanda_p)-1):
            yhat = Predict.predic(model, x_test[i:i+1])
            demanda_p[i] = yhat

            if i == x_test.shape[0]-2:
                break
            
            elif i == x_test.shape[0]-71:
                for j in range(1,70):
                    x_test[f'lag{j}'][i+j]=yhat
            
            elif i == x_test.shape[0]-70:
                for j in range(1,69):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-69:
                for j in range(1,68):
                    x_test[f'lag{j}'][i+j]=yhat
            
            elif i == x_test.shape[0]-68:
                for j in range(1,67):
                    x_test[f'lag{j}'][i+j]=yhat
                
            elif i == x_test.shape[0]-67:
                for j in range(1,66):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-66:
                for j in range(1,65):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-65:
                for j in range(1,64):
                    x_test[f'lag{j}'][i+j]=yhat
            
            elif i == x_test.shape[0]-64:
                for j in range(1,63):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-63:
                for j in range(1,62):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-62:
                for j in range(1,61):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-61:
                for j in range(1,60):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-60:
                for j in range(1,59):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-59:
                for j in range(1,58):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-58:
                for j in range(1,57):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-57:
                for j in range(1,56):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-56:
                for j in range(1,55):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-55:
                for j in range(1,54):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-54:
                for j in range(1,53):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-53:
                for j in range(1,52):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-52:
                for j in range(1,51):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-51:
                for j in range(1,50):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-50:
                for j in range(1,49):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-49:
                for j in range(1,48):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-48:
                for j in range(1,47):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-47:
                for j in range(1,46):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-46:
                for j in range(1,45):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-45:
                for j in range(1,44):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-44:
                for j in range(1,43):
                    x_test[f'lag{j}'][i+j]=yhat
            
            elif i == x_test.shape[0]-43:
                for j in range(1,42):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-42:
                for j in range(1,41):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-41:
                for j in range(1,40):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-40:
                for j in range(1,39):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-39:
                for j in range(1,38):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-38:
                for j in range(1,37):
                    x_test[f'lag{j}'][i+j]=yhat
       
            elif i == x_test.shape[0]-37:
                for j in range(1,36):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-36:
                for j in range(1,35):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-35:
                for j in range(1,34):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-34:
                for j in range(1,33):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-33:
                for j in range(1,32):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-32:
                for j in range(1,31):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-31:
                for j in range(1,30):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-30:
                for j in range(1,29):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-29:
                for j in range(1,28):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-28:
                for j in range(1,27):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-27:
                for j in range(1,26):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-26:
                for j in range(1,25):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-25:
                for j in range(1,24):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-24:
                for j in range(1,23):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-23:
                for j in range(1,22):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-22:
                for j in range(1,21):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-21:
                for j in range(1,20):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-20:
                for j in range(1,19):
                    x_test[f'lag{j}'][i+j]=yhat
    
            elif i == x_test.shape[0]-19:
                for j in range(1,18):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-18:
                for j in range(1,17):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-17:
                for j in range(1,16):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-16:
                for j in range(1,15):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-15:
                for j in range(1,14):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-14:
                for j in range(1,13):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-13:
                for j in range(1,12):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-12:
                for j in range(1,11):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-11:
                for j in range(1,10):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-10:
                for j in range(1,9):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-9:
                for j in range(1,8):
                    x_test[f'lag{j}'][i+j]=yhat

            elif i == x_test.shape[0]-8:
                for j in range(1,7):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-7:
                for j in range(1,6):
                    x_test[f'lag{j}'][i+j]=yhat
        
            elif i == x_test.shape[0]-6:
                for j in range(1,5):
                    x_test[f'lag{j}'][i+j]=yhat
                    
            elif i == x_test.shape[0]-5:
                for j in range(1,4):
                    x_test[f'lag{j}'][i+j]=yhat
       
            elif i == x_test.shape[0]-4:
                x_test['lag1'][i+1] = yhat
                x_test['lag2'][i+2] = yhat 

            elif i == x_test.shape[0]-3:
                x_test['lag1'][i+1] = yhat
                
            else:
                for j in range(1,71):
                    x_test[f'lag{j}'][i+j]=yhat
                    
        new_order = ['Carnaval','lag1', 'lag2', 'lag3', 'lag4','lag5','lag6','lag7','lag8','lag9','lag10','lag11','lag12','lag13','lag14',
             'lag15','lag16','lag17','lag18','lag19','lag20','lag21','lag22','lag23','lag24','lag25','lag26','lag27','lag28','lag29','lag30','lag31','lag32','lag33','lag34','lag35','lag36','lag37','lag38','lag39','lag40',
             'lag41','lag42','lag43','lag44','lag45','lag46','lag47','lag48','lag49','lag50','lag51','lag52','lag53','lag54','lag55','lag56','lag57','lag58','lag59','lag60','lag61','lag62','lag63','lag64','lag65','lag66',
             'lag67','lag68','lag69','lag70',
             'FDS', 'Festivos','demanda_1', 'demanda_2', 'demanda_3', 'demanda_4', 'demanda_5', 'demanda_6','demanda_7', 'demanda_8', 'demanda_9', 'demanda_10', 'demanda_11', 'demanda_12','Marca_Hora_P','wday_2', 'wday_3', 'wday_4', 'wday_5', 'wday_6', 'wday_7','wday_1',
             'Verano','Otoño','Invierno','Primavera' ]
        
        x_test= x_test.reindex(columns=new_order)
        
        #demanda_p[x_test.shape[0]-1] = Predict.predic(model, x_test[x_test.shape[0]-1:x_test.shape[0]])
        
        return demanda_p


