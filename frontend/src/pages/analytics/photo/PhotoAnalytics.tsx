import { LineChartComponent } from '@/components/charts/LineChart';
import styles from '@/pages/common.module.css';
import analytics_styles from '../analytics.module.css';
import { useEffect, useState } from 'react';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { getPhotoTakenCount, getPhotoPostedCount, getPhotoPerMonthByYear, getPhotoCountHour, getAvgTimeToPost } from '@/components/api/api';
import { AvgData, HourCountData, MonthData, YearData } from '@/lib/types';
import { BarChart } from '@/components/charts/BarChart';
import { GaugeChart } from '@/components/charts/GaugeChart';

export function PhotoAnalytics() {
  /*Data Taken*/
  const [yearTakenLabels, setYearTakenLabels] = useState<number[]>([]);
  const [yearTakenData, setYearTakenData] = useState<number[]>([]);
  const [monthTakenLabels, setMonthTakenLabels] = useState<string[]>([]);
  const [monthTakenData, setMonthTakenData] = useState<number[]>([]);

  const [isTakenLoading, setIsTakenLoading] = useState<boolean>(true);

  /*Data Posted*/
  const [yearPostedLabels, setYearPostedLabels] = useState<number[]>([]);
  const [yearPostedData, setYearPostedData] = useState<number[]>([]);
  const [monthPostedLabels, setMonthPostedLabels] = useState<string[]>([]);
  const [monthPostedData, setMonthPostedData] = useState<number[]>([]);

  const [isPostedLoading, setIsPostedLoading] = useState<boolean>(true);

  /*Photo per hour */
  const [hourTaken, setHourTaken] = useState<HourCountData[]>([])
  const [hourPosted, setHourPosted] = useState<HourCountData[]>([])
  const [isHourLoading, setIsHourLoading] = useState<boolean>(true);

  /*Months per years*/
  const [yearLabels, setYearLabels] = useState<number[]>([]);
  const [selectedYear, setSelectedYear] = useState<number>(2004);
  const [selectedType, setSelectedType] = useState<string>('Postate');

  const [monthlyData, setMonthlyData] = useState<{
    month: string;
    count: number;
    trend: {
      value: number;
      direction: 'up' | 'down';
    };
  }[]>([]);
  const [isMonthlyDataLoading, setIsMonthlyDataLoading] = useState<boolean>(true);

  /*AvgTimeToPost*/
  const [avg, setAvg] = useState<AvgData>({averageTimeToPostMinutes: 294975.495650755});
  const [isAvgLoading, setIsAvgLoading] = useState<boolean>(true);

  useEffect(() => {
    const fetchTakenData = async () => {
      setIsTakenLoading(true);
      try {
        const data = await getPhotoTakenCount();

        setYearTakenLabels(data.year_data.map((item: YearData) => item[0]));
        setYearTakenData(data.year_data.map((item: YearData) => item[1]));

        setMonthTakenLabels(data.month_data.map((item: MonthData) => item[0]));
        setMonthTakenData(data.month_data.map((item: MonthData) => item[1]));
      } catch (error) {
        console.log('Errore nel recuperare i dati sulle foto scattate: ', error);
      } finally {
        setIsTakenLoading(false);
      }
    };

    const fetchPostedData = async () => {
      setIsPostedLoading(true);
      try {
        const data = await getPhotoPostedCount();

        setYearPostedLabels(data.year_data.map((item: YearData) => item[0]));
        setYearPostedData(data.year_data.map((item: YearData) => item[1]));

        setMonthPostedLabels(data.month_data.map((item: MonthData) => item[0]));
        setMonthPostedData(data.month_data.map((item: MonthData) => item[1]));
      } catch (error) {
        console.log('Errore nel recuperare i dati sulle foto scattate: ', error);
      } finally {
        setIsPostedLoading(false);
      }
    };

    const fetchHourData = async () =>{
      setIsHourLoading(true);
      try {
        const data = await getPhotoCountHour();
        setHourPosted(data.posted);
        setHourTaken(data.taken);
      } catch (error) {
        console.log('Errore nel recuperare i dati sulle foto per ora: ', error);
      } finally {
        setIsHourLoading(false);
      }
    }

    const fetchAvgData = async () =>{
      setIsAvgLoading(true);
      try {
        const data = await getAvgTimeToPost();
        setAvg(data[0]);
      } catch (error) {
        console.log('Errore nel recuperare il tempo medio: ', error);
      } finally {
        setIsAvgLoading(false);
      }
    }

    fetchTakenData();
    fetchPostedData();
    fetchHourData();
    fetchAvgData();
  }, []);

  useEffect(() => {
    const fetchMonthlyData = async () => {
      try {
        setIsMonthlyDataLoading(true);
        const type = selectedType == 'Postate' ? 'posted' : 'taken';
        const data = await getPhotoPerMonthByYear(selectedYear, type);
        const processedData = data.map((item: any, index: number, arr: any[]) => {
          const previousCount = index > 0 ? arr[index - 1].count : 0;
          const difference = index > 0 ? item.count - previousCount : 0;
          return {
            month: item.month,
            count: item.count,
            trend: {
              value: Math.abs(difference),
              direction: difference >= 0 ? 'up' : 'down',
            },
          };
        }) as { month: string; count: number; trend: { value: number; direction: 'up' | 'down' } }[];
        setMonthlyData(processedData);
      } catch (error) {
        console.error('Errore nel recupero dei dati mensili per anno:', error);
      } finally {
        setIsMonthlyDataLoading(false); // Fine caricamento
      }
    };
    fetchMonthlyData();
  }, [selectedYear, selectedType]);

  useEffect(() => {
    if (selectedType === 'Postate') {
      setYearLabels(yearPostedLabels);
    } else {
      setYearLabels(yearTakenLabels);
    }
  }, [selectedType, yearPostedLabels, yearTakenLabels]);

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h2 className={styles.title}>Analisi e tendenze delle foto</h2>
      </div>
      <hr></hr>

      <div>
        <h1 className={styles.subtitle}>Foto scattate</h1>
        <div className={styles.explanation}>
          <p>
            I grafici seguenti illustrano le tendenze relative ai periodi in cui vengono scattate più foto. <br />
            Analizzando i dati annuali e mensili, è possibile individuare gli anni e i mesi con la maggiore attività fotografica, fornendo una visione chiara dei momenti più significativi per la cattura di immagini.
          </p>
        </div>
        <div className={analytics_styles.chartsGrid}>
          <div className={analytics_styles.chartContainer}>
            {isTakenLoading ? (
              <LoadingSpinner />
            ) : (
              <LineChartComponent
                lineColor='rgb(0, 128, 191)'
                labels={yearTakenLabels.map(String)}
                data={yearTakenData}
                title="Foto scattate annualmente"
              />
            )}
          </div>
          <div className={analytics_styles.chartContainer}>
            {isTakenLoading ? (
              <LoadingSpinner />
            ) : (
              <LineChartComponent
                lineColor='rgb(255, 87, 34)'
                labels={monthTakenLabels}
                data={monthTakenData}
                title="Foto scattate mensilmente"
              />
            )}
          </div>
        </div>
        <hr></hr>
      </div>
      <div >
        <h1 className={styles.subtitle}>Foto pubblicate</h1>
        <div className={styles.explanation}>
          <p>
            I grafici qui presentati mostrano i periodi in cui vengono pubblicate più foto. <br />
            Attraverso l'analisi dei dati annuali e mensili, possiamo osservare i momenti in cui l'attività di pubblicazione raggiunge i suoi picchi, evidenziando le tendenze e le abitudini legate alla condivisione di immagini.
          </p>
        </div>
        <div className={analytics_styles.chartsGrid}>
          <div className={analytics_styles.chartContainer}>
            {isPostedLoading ? (
              <LoadingSpinner />
            ) : (
              <LineChartComponent
                lineColor='rgb(34, 153, 84)'
                labels={yearPostedLabels.map(String)}
                data={yearPostedData}
                title="Foto postate annualmente"
              />
            )}
          </div>
          <div className={analytics_styles.chartContainer}>
            {isPostedLoading ? (
              <LoadingSpinner />
            ) : (
              <LineChartComponent
                lineColor='rgb(255, 193, 7)'
                labels={monthPostedLabels}
                data={monthPostedData}
                title="Foto postate mensilmente"
              />
            )}
          </div>
        </div>
        <hr></hr>
      </div>
        <div>
          <h1 className={styles.subtitle}>Esplora le tendenze delle foto mese per mese</h1>
          <div className={styles.explanation}>
            <p>
              Seleziona un anno e una tipologia di dati (scatto o pubblicazione) per visualizzare le statistiche dettagliate. <br />
              Questa analisi aiuta a identificare i periodi con maggiore attività nell'anno selezionato.
            </p>
          </div>
          <div className={analytics_styles.selectGrid}>
            <div className={analytics_styles.selectContainer}>
              <label htmlFor="year-select" className={analytics_styles.label}>Anno: </label>
              <select
                id="year-select"
                value={selectedYear}
                onChange={(e) => setSelectedYear(parseInt(e.target.value, 10))}
                className={analytics_styles.select}
                required
              >
                <option value="" disabled>
                  Seleziona un anno
                </option>
                {yearLabels.map((year) => (
                  <option key={year} value={year}>
                    {year}
                  </option>
                ))}
              </select>
            </div>
            <div className={analytics_styles.selectContainer}>
              <label htmlFor="type-select" className={analytics_styles.label}>Tipo: </label>
              <select
                id="type-select"
                value={selectedType}
                onChange={(e) => setSelectedType(e.target.value)}
                className={analytics_styles.select}
                required
              >
                <option value="Postate">Postate</option>
                <option value="Scattate">Scattate</option>
              </select>
            </div>
          </div>
          <div className={styles.content}>
            {isMonthlyDataLoading ? (
              <div className={analytics_styles.loadingContainer}>
                <LoadingSpinner />
              </div>
            ) : (
              <div className={analytics_styles.monthsGrid}>
                {monthlyData.map((item) => (
                  <div key={item.month} className={analytics_styles.monthCard}>
                    <h3>{item.month}</h3>
                    <p style={{ fontSize: '24px', fontWeight: 'bold' }}>{item.count}</p>
                    <p style={{ color: item.trend.direction === 'up' ? 'green' : 'red' }}>
                      {item.trend.value} {item.trend.direction === 'up' ? '↑' : '↓'}
                    </p>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
        <hr></hr>
      <div>
        <h1 className={styles.subtitle}>Distribuzione oraria delle foto pubblicate e scattate</h1>
        <div className={styles.explanation}>
          <p>
          Il grafico seguente mostra la distribuzione oraria delle foto pubblicate e scattate. <br/>
          Questa analisi evidenzia i momenti della giornata con maggiore attività, permettendo di individuare gli orari più comuni per visitare la citta e per condividere delle immagini.
          </p>
        </div>
        <div className={analytics_styles.chartsGrid}>
          <div className={analytics_styles.chartContainer}>
            {isHourLoading ? (
              <LoadingSpinner />
            ) : (
              <BarChart hourData={hourPosted} title='Orario di pubblicazione'/>
            )}
          </div>
          <div className={analytics_styles.chartContainer}>
            {isHourLoading ? (
              <LoadingSpinner />
            ) : (
              <BarChart hourData={hourTaken} barColor='rgba(250, 96, 96, 0.8)' title='Orario di scatto'/>
            )}
          </div>
        </div>
      </div>
      <hr/>
      <div>
        <div className={styles.explanation}>
          <h1 className={styles.subtitle}>Tempo Medio di Pubblicazione</h1>
          <p>
            Media del tempo trascorso tra lo scatto e la pubblicazione.
          </p>
        </div>
          <div className={analytics_styles.gaugeContainer}>
            {isAvgLoading? (
              <LoadingSpinner />
            ) : (
              <GaugeChart avg={avg}/>
            )}
          </div>
      </div>
    </div>
  );
}
