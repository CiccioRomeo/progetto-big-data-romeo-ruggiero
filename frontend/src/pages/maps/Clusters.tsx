import { useState } from 'react';
import { ClusterControls } from '@/components/maps/clusters/cluster-view/ClusterControl';
import { ClusterMapView } from '@/components/maps/clusters/cluster-view/ClusterMapView';
import { KMeans } from '@/components/api/api';
import styles from '../common.module.css';
import map_styles from './map.module.css';
import type { Centroids, CentroidsData, Cluster, ClusterData, Distances, KMeansResponse } from '@/lib/types';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { ArcMapView } from '@/components/maps/clusters/distances-view/ArcMapView';

export function Clusters() {
  const [k, setK] = useState<number>(5);
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [centroids, setCentroids] = useState<Centroids[]>([]);
  const [distances, setDistances] = useState<Distances[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);  

  const fetchClusterData = async (clusters: number) => {
    setIsLoading(true);
    try {
      const kMeansResponse: KMeansResponse = await KMeans(clusters);
      const clusterData: ClusterData[] = kMeansResponse.labels;
      const mappedCluster: Cluster[] = clusterData
        .filter(data => Array.isArray(data) && data.length >= 3) // Filtra dati non validi
        .map(data => ({
          latitude: data[0],
          longitude: data[1],
          label: data[2]           
        }));
      const centroidsData: CentroidsData[] = kMeansResponse.centroids;
      const mappedCentroids: Centroids[] = centroidsData
      .filter(data => Array.isArray(data) && data.length >= 2) // Filtra dati non validi
      .map(data => ({
        latitude: data[0],
        longitude: data[1],    
      }));

      setClusters(mappedCluster);
      setCentroids(mappedCentroids);
      setDistances(kMeansResponse.distanze);
      //setClusters(MOCK_DATA);
      //setCentroids(MOCK_CENTROIDS)
    } catch (error) {
      console.error('Errore nel recupero dei dati dei cluster:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleVisualizza = (clusters: number) => {
    setK(clusters); // Aggiorna lo stato di k
    fetchClusterData(clusters); // Recupera i dati
  };

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h2 className={styles.title}>Mappa dei Cluster: analisi geografica</h2>
      </div>
      <hr />
      <div>
        <p className={styles.text}>
        Ogni punto sulla mappa rappresenta una posizione geografica e i punti con lo stesso colore appartengono allo stesso cluster. <br/>
        I cluster sono determinati utilizzando l'algoritmo k-means, che identifica i centroidi e assegna ogni punto al cluster più vicino in base alle coordinate geografiche.
        </p>
      </div>
      <div>
        <ClusterControls k={k} onVisualizza={handleVisualizza} />
      </div>
      <div className={map_styles.mapWrapper}>
      {isLoading ? (
            <LoadingSpinner />
          ) : (
          <ClusterMapView clusters={clusters} centroids={centroids} isLoading={isLoading} />
        )
      }
      </div>
      <hr />
      <div>
        <p className={styles.text}>
        Nella mappa sottostante sono rappresentate le distanze in linea d'aria tra i centroidi dei cluster e i diversi monumenti. <br/>
        Il menu laterale permette di selezionare o deselezionare i cluster per una visualizzazione più chiara. Passando con il cursore sugli archi, è possibile visualizzare un tooltip con informazioni dettagliate.
        </p>
      </div>
      <div className={map_styles.mapWrapper}>
      {isLoading ? (
            <LoadingSpinner />
          ) : (
         <ArcMapView data={distances}/>
        )
      }
      </div>
    </div>
  );
}
