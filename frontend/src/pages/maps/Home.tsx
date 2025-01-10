import { MapView } from '@/components/maps/deckgl/MapView';
import styles from '../common.module.css';
import map_styles from './map.module.css';
import { useEffect, useState } from 'react';
import { Accuracy, AvgCommentsYear, AvgMdnComments, AvgMdnViews, AvgViewsYear, Coordinate, CoordinateData, TotalCameraBrands, TotalRows, TotalUsers } from '@/lib/types';
import { getCoordinates, 
  getTotalRows, 
  getTotalUsers,
  getTotalCameras,
  getViewsStats,
  getCommentsStats,
  getAvgViewsPerYear,
  getAvgCommentsPerYear,
  getAccuracyDistribution
} from '@/components/api/api';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { StatsOverview } from '@/components/home/StatsOverview';

export function Home() {
  const [rows, setRows] = useState<TotalRows>(0);
  const [users, setUsers] = useState<TotalUsers>(0);
  const [brands, setBrands] = useState<TotalCameraBrands>(0);
  const [views, setViews] = useState<AvgMdnViews>({
    average_views: 0,
    median_views: 0,
  });
  const [comments, setComments] = useState<AvgMdnComments>({
    average_comments: 0,
    median_comments: 0,
  });
  const [viewsYear, setViewsYear] = useState<AvgViewsYear[]>([]);
  const [commentsYear, setCommentsYear] = useState<AvgCommentsYear[]>([]);
  const [accuracy, setAccuracy] = useState<Accuracy[]>([]);

  const [coordinates, setCoordinates] = useState<Coordinate[]>([]);

  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [isDashboradLoading, setIsDashboradLoading] = useState<boolean>(true);
  
  useEffect(() => {

    const fethData = async () => {
      setIsDashboradLoading(true);
      try{
        const rowsData = await getTotalRows();
        setRows(rowsData);
        const usersData = await getTotalUsers();
        setUsers(usersData);
        const brandsData = await getTotalCameras();
        setBrands(brandsData);
        const viewsData = await getViewsStats();
        setViews(viewsData[0]);
        const commentsData = await getCommentsStats();
        setComments(commentsData[0]);
        const viewYearData = await getAvgViewsPerYear();
        setViewsYear(viewYearData);
        const commentsYearData = await getAvgCommentsPerYear();
        setCommentsYear(commentsYearData);
        const accuracyData = await getAccuracyDistribution();
        setAccuracy(accuracyData);
      } catch(error){
        console.error('Errore nel recupero dei dati:', error);
      } finally {
        setIsDashboradLoading(false);
      }
    }

    const fetchCoordinates = async () => {
      setIsLoading(true);
      try {
        const coordinatesData: CoordinateData[] = await getCoordinates();
        console.log('Dati originali:', coordinatesData);

        const mappedCoordinates: Coordinate[] = coordinatesData
          .filter(data => Array.isArray(data) && data.length >= 3) // Filtra dati non validi
          .map(data => ({
            position: [data[1], data[0]], // [longitude, latitude]
            intensity: data[2]           // intensity
          }));
        
        console.log('Dati mappati:', mappedCoordinates);
        
        setCoordinates(mappedCoordinates);
        
        console.log(coordinates.length);
      } catch (error) {
        console.error('Errore nel recupero dei dati annuali:', error);
      } finally {
        setIsLoading(false);
      }
    };

    fethData();
    fetchCoordinates();
  }, []);

  return (
    <div className={styles.page}>
      <div className={styles.container}>
        <div className={styles.header}>
          <h2 className={styles.title}>Homepage</h2>
        </div>
        <hr></hr>
        <div className={styles.explanation}>
          <h2 className={styles.subtitle}>Informazioni sul dataset</h2>
          <p>
          Il dataset contiene fotografie geolocalizzate di Roma raccolte tra il 2000 e il 2017. <br/>
            Le righe sono state filtrate e processate per garantire la qualità e la rilevanza 
            dei dati, mantenendo solo le foto più significative e rimuovendo contenuti duplicati 
            o non pertinenti.
          </p>
        <div  className={styles.content}>
        {isDashboradLoading ? 
          <LoadingSpinner/>
          :(
            <StatsOverview
              totalRows={rows}
              totalUsers={users}
              totalBrands={brands}
              viewsStats={views}
              commentsStats={comments}
              yearlyViews={viewsYear}
              yearlyComments={commentsYear}
              accuracyDistribution={accuracy}
            />
          )
        }
        </div>
        </div>
        <hr></hr>
        <div>
        <h2 className={styles.subtitle}>Distribuzione geografica delle foto</h2>
          <p className={styles.text}>
            Esplora la distribuzione geografica dei post di Flickr con foto geotaggate.<br />
            Ogni esagono rappresenta un'area che aggrega le foto pubblicate, 
            permettendoti di scoprire i luoghi più documentati e popolari.
          </p>
        </div>
        <div className={map_styles.mapWrapper}>
          {isLoading ? (
              <LoadingSpinner />
            ) : (
            <MapView coordinates={coordinates}/>
            )
          }
        </div>
      </div>
    </div>
  );
}
