import photo_search_styles from './photo-search.module.css';
import styles from '../common.module.css';
import { SearchFilters } from '@/components/analytics/photos/SearchFilters';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useState } from 'react';
import { PhotoSearched, PhotoSearchFilters } from '@/lib/types';
import { PhotoCard } from '@/components/analytics/photos/PhotoCard';
import { getPhotos } from '@/components/api/api';

export function PhotoSearch() {
    const [isSearching, setIsSearching] = useState<boolean>(false);
    const [photos, setPhotos] = useState<PhotoSearched[]>([]);
  
    const searchPhotos = async (filters: PhotoSearchFilters) => {
      setIsSearching(true);
      try {
        const photosData: PhotoSearched[] = await getPhotos(filters.keyword, filters.startDate, filters.endDate, filters.tags);
        setPhotos(photosData);
      } catch (error) {
        console.error('Errore nel recupero delle foto: ', error);
      } finally {
        setIsSearching(false);
      }
    };
  
    return (
      <div className={styles.container}>
        <div className={styles.header}>
          <h1 className={styles.title}>Ricerca delle foto</h1>
        </div>
        <hr></hr>
        <div>
              <SearchFilters onSearch={searchPhotos} />
            </div>
            <div className={photo_search_styles.results}>
              {isSearching ? (
                <div className={photo_search_styles.loading}>
                  <LoadingSpinner />
                </div>
              ) : photos.length > 0 ? (
                <div className={photo_search_styles.grid}>
                  {photos.map(photo => (
                    <div className={photo_search_styles.cardWrapper}>
                      <PhotoCard photo={photo} />
                    </div>
                  ))}
                </div>
              ) : (
                <div className={photo_search_styles.noResults}>
                  Non è stata trovata nessuna foto che soddisfi i filtri!
                </div>
              )}
            </div>
      </div>
    );
  }
  