import { OnwerCard } from "@/components/analytics/users/OwnerCard";
import top_owner_styles from './top-owners.module.css';
import styles from '@/pages/common.module.css';
import { useState, useEffect } from "react";
import type { FirstPost, OwnerSearched, PopularOwner, UserSearchFilters } from "@/lib/types";
import { getFirstPostPerYear, getOwners, getPopularOwners, getProVsNonPro } from "@/components/api/api";
import { LoadingSpinner } from "@/components/ui/LoadingSpinner";
import { UsersChart } from "@/components/charts/UsersChart";
import { OwnerSearchFilters } from "@/components/analytics/users/OwnerSearchFilters";
import photo_search_styles from "@/components/users-photos/photo-search/search-filters.module.css";
import { DoughnutChart } from "@/components/charts/DoughnutChart";
import { BubbleChart } from "@/components/charts/BubbleChart";

export function UsersAnalytics() {
  const [owners, setOwners] = useState<PopularOwner[]>([]);
  const [searchedOwners, setSearchedOwners] = useState<OwnerSearched[]>([]);
  const [firstPosts, setFirstPosts] = useState<FirstPost[]>([]);
  const [proVsNonPro, setProVsNonPro] = useState<number[]>([]);
  const [isSearching, setIsSearching] = useState(true);
  const [isPOLoading, setIsPOLoading] = useState(true);
  const [isFPYLoading, setIsFPYLoading] = useState(true);
  const [isProVsNonProLoading, setIsProVsNonProLoading] = useState(true);
  const [searchClicked, setIsSearchClicked] = useState(false);

  const searchOwner = async (filters: UserSearchFilters) => {
    setIsSearching(true);
    setIsSearchClicked(true);
    try {
      const data = await getOwners(filters.owner);
      console.log('Owners cercati: ',data)
      setSearchedOwners(data);
    } catch (error) {
      console.error("Errore nel recupero dei dati", error);
    } finally {
      setIsSearching(false);
    }
  };

  useEffect(() => {
    const fetchPopularOwners = async () => {
      setIsPOLoading(true);
      try {
        const data = await getPopularOwners();
        console.log('Owners ricevuti: ',data)
        setOwners(data);
      } catch (error) {
        console.error("Errore nel recupero dei dati", error);
      } finally {
        setIsPOLoading(false);
      }
    };

    const fetchFirstPosts = async () => {
      setIsFPYLoading(true);
      try {
        const data = await getFirstPostPerYear();
        console.log('First posts ricevuti: ',data)
        setFirstPosts(data);
      } catch (error) {
        console.error("Errore nel recupero dei dati", error);
      } finally {
        setIsFPYLoading(false);
      }
    };

    const fetchProNonProData = async () => {
      setIsProVsNonProLoading(true);
      try {
          const data = await getProVsNonPro();
          const result = [0,0]
          for (const row of data) {
            if (row.pro) {
              result[0] = row.user_count;
            } else {
              result[1] = row.user_count;
            }
          }
          console.log('First posts ricevuti: ',data);
          setProVsNonPro(result);
        } catch (error) {
          console.error("Errore nel recupero dei dati", error);
        } finally {
          setIsProVsNonProLoading(false);
        }
      };
    
    fetchFirstPosts();
    fetchPopularOwners();
    fetchProNonProData();
  }, []);

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h2 className={styles.title}>Analisi degli utenti</h2>
      </div>
      <hr />
      <div className={styles.explanation}>
        <h1 className={styles.subtitle}>Gli utenti più popolari</h1>
        <p>
          Scopri i 50 utenti più popolari di Flickr attraverso un'analisi dettagliata dei loro contenuti.
          <br />
          Nel grafico a bolle, l'asse delle ordinate rappresenta il numero di commenti ricevuti, l'asse delle ascisse indica il numero di like, mentre la dimensione di ciascuna bolla corrisponde al totale delle visualizzazioni dei loro contenuti.
        </p>
      </div>
      <div className={top_owner_styles.chartContainer}>
        {isPOLoading ? (
          <LoadingSpinner />
        ) : (
          <UsersChart owners={owners} />
        )}
      </div>
      <hr />
      <div className={styles.explanation}>
        <h1 className={styles.subtitle}>Distribuzione temporale delle prime foto pubblicate</h1>
        <p>
          In questa sezione viene utilizzato un grafico a bolle per analizzare la distribuzione temporale delle prime foto pubblicate dagli utenti. <br/>
          La dimensione delle bolle riflette il numero di utenti che hanno pubblicato la loro prima foto in un determinato mese e anno, offrendo una visione chiara e intuitiva delle tendenze temporali e stagionali.
        </p>
      </div>
      <div className={top_owner_styles.chartContainer}>
        {isFPYLoading ? (
          <LoadingSpinner />
        ) : (
          <BubbleChart data={firstPosts}/>
        )}
      </div>
      <hr />
      <div className={styles.explanation}>
        <h1 className={styles.subtitle}>Distribuzione degli utenti PRO e Non-PRO su Flickr</h1>
        <p>
        Scopri la suddivisione tra utenti PRO e non-PRO su Flickr attraverso un grafico a ciambella. <br /> 
        Gli utenti PRO su Flickr sono coloro che hanno sottoscritto un abbonamento premium, il quale offre vantaggi esclusivi come spazio di archiviazione illimitato, strumenti avanzati per la gestione delle foto e un'esperienza senza pubblicità. <br /> 
        Questo grafico mostra la percentuale degli utenti che hanno scelto il piano PRO rispetto a quelli che utilizzano la versione gratuita della piattaforma.
        </p>
        <div>
          <div className={top_owner_styles.doughnutContainer}>
            {isProVsNonProLoading ? (
              <LoadingSpinner />
            ) : (
              <DoughnutChart
                labels={['PRO', 'Non-PRO']}
                values={proVsNonPro}
              />
            )}
          </div>
        </div>
        <hr />
        <div className={styles.explanation}>
          <h1 className={styles.subtitle}>Ricerca Utente</h1>
          <p>
            Vuoi saperne di più su un fotografo specifico? Cerca il suo nome e ottieni immediatamente le informazioni sulla sua foto più visualizzata. Scopri cosa rende speciale il loro contenuto e lasciati ispirare dai loro risultati.
          </p>
        </div>
        <div>
          <OwnerSearchFilters onSearch={searchOwner} />
        </div>
        <div className={photo_search_styles.results}>
          {searchClicked ? (
            isSearching ? (
              <div className={photo_search_styles.loading}>
                <LoadingSpinner />
              </div>
            ) : searchedOwners.length ? (
              <div className={top_owner_styles.grid}>
                {searchedOwners.map((owner) => (
                  <OnwerCard owner={owner} />
                ))}
              </div>
            ) : (
              <div className={photo_search_styles.noResults}>
                Non è stata trovato nessun utente che soddisfa i filtri!
              </div>
            )
          ) : (
            <div className={photo_search_styles.noResults}>
              Ottieni le informazioni sull'utente che ti interessa!
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
