import styles from '@/pages/common.module.css';
import analytics_styles from '../analytics.module.css';
import photo_search_styles from "@/components/users-photos/photo-search/search-filters.module.css";
import { useEffect, useState } from 'react';
import { CameraSearch, SearchedCamera, TopBrandAndModels, TopCameraPerYear } from '@/lib/types';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { getCamerasInfo, getTopBrandsAndModels, getTopCamerasPerYear } from '@/components/api/api';
import { CameraSearchFilters } from '@/components/analytics/camera/CameraSearchFilters';
import { CameraLineChart } from '@/components/charts/CameraLineChart';
import { CameraBarChart } from '@/components/charts/CameraBarChart';
import { CameraCard } from '@/components/analytics/camera/CameraCard';

export function CameraAnalytics(){
    const [topCameras, setTopCameras] = useState<TopCameraPerYear[]>([]);
    const [topBrandsAndModels, setTopBrandsAndModels] = useState<TopBrandAndModels[]>([]);
    const [searchedCameras, setSearchedCameras] = useState<SearchedCamera[]>([])
    const [isLoading, setIsLoading] = useState<boolean>(true);
    const [isSearching, setIsSearching] = useState<boolean>(false);
    const [searchClicked, setIsSearchClicked] = useState<boolean>(true);

    const searchCameras = async (filters: CameraSearch) => {
        setIsSearching(true);
        setIsSearchClicked(true);
        try {
          const data = await getCamerasInfo(filters.brand);
          setSearchedCameras(data);
        } catch (error) {
          console.error("Errore nel recupero dei dati", error);
        } finally {
          setIsSearching(false);
        }
    };

    useEffect(() => {
        const fetchTopCamera = async () => {
            setIsLoading(true);
            try{
                const data = await getTopCamerasPerYear();
                console.log('Camere ricevute: ',data)
                setTopCameras(data);
            }   catch (error){
                console.error("Errore nel recupero dei dati", error)
            } finally{
                setIsLoading(false);
            }
        }

        const fetchTopBrands = async () => {
          setIsLoading(true);
          try{
              const data = await getTopBrandsAndModels();
              console.log('Camere ricevute: ',data)
              setTopBrandsAndModels(data);
          }   catch (error){
              console.error("Errore nel recupero dei dati", error)
          } finally{
              setIsLoading(false);
          }
      }

        fetchTopCamera();
        fetchTopBrands();
    }, []);

    return(
        <div className={styles.container}>
            <div className={styles.header}>
                <h2 className={styles.title}>Analisi delle fotocamere</h2>
            </div>
            <hr/>
            <div className={styles.explanation}>
                <h1 className={styles.subtitle}>Le fotocamere più usate dagli utenti</h1>
                <p>
                Grazie ai dati del nostro dataset e alle API di Flickr, siamo riusciti a scoprire quali fotocamere gli utenti hanno usato di più nel corso degli anni. 
                Qui sotto puoi vedere un grafico che mostra i modelli più popolari anno per anno.
                </p>
            </div>
            <div className={analytics_styles.chartContainer}>
            {isLoading ? (
              <LoadingSpinner />
            ) : (
              <CameraLineChart cameras={topCameras}/>
            )}
            </div>
            <hr/>
            <div className={styles.explanation}>
                <h1 className={styles.subtitle}>I migliori brand</h1>
                <p>
                Ecco i 4 brand di fotocamere più utilizzati dagli utenti! In questa sezione, ti mostriamo i marchi che dominano il panorama delle fotografie e i loro 5 modelli più popolari. 
                </p>
            </div>
            {isLoading ? (
              <LoadingSpinner />
            ) : (
            <div className={analytics_styles.cameraCardGrid}>
              {topBrandsAndModels.map(camera =>
                  <CameraCard camera={camera}/>
              )}   
                </div>
              
            )}
            <hr/>
            <div className={styles.explanation}>
                <h1 className={styles.subtitle}>Scopri l'utilizzo delle fotocamere per ogni brand</h1>
                <p>
                Esplora i modelli di fotocamere del tuo brand preferito e scopri il loro utilizzo in un colpo d'occhio! <br/>
                Inserisci il nome di un brand per visualizzare il numero di foto scattate con ogni modello di quel brand rappresentato in un radar plot interattivo. <br/>
                Uno strumento semplice e visivo per analizzare l'impatto e la popolarità dei modelli sul mercato.
                </p>
            </div>
            <div>
                <CameraSearchFilters onSearch={searchCameras}/>
            </div>
            <div className={photo_search_styles.results}>
          {searchClicked ? (
            isSearching ? (
              <div className={photo_search_styles.loading}>
                <LoadingSpinner />
              </div>
            ) : searchedCameras.length ? (
              <div className={analytics_styles.chartContainer}>
                <CameraBarChart cameras={searchedCameras}/>
              </div>
            ) : (
              <div className={photo_search_styles.noResults}>
                Non è stata trovata nessuna fotocamera che soddisfi i filtri!
              </div>
            )
          ) : (
            <div className={photo_search_styles.noResults}>
              Ottieni le informazioni sul brand che ti interessa!
            </div>
          )}
        </div>
        </div>
    );
}
