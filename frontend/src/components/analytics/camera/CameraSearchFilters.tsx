import { useState } from 'react';
import styles from '@/components/users-photos/photo-search/search-filters.module.css';
import { CameraSearch } from '@/lib/types';

interface SearchFiltersProps{
    onSearch: (filters: CameraSearch) => void;
}

export function CameraSearchFilters({ onSearch }: SearchFiltersProps ){

    const [filters, setFilters] = useState<CameraSearch>({
        brand: ''
    });
    
    const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSearch(filters);
    };
    
    return (
        <form onSubmit={handleSubmit} className={styles.form}>
            <div className={styles.textFilters}>
                <div className={styles.input}>
                    <label htmlFor='brand'>Brand</label>
                    <input 
                        type='text' 
                        id='brand' 
                        placeholder='Cerca...' 
                        value={filters.brand}
                        onChange={e => setFilters(prev => ({
                            ...prev,
                            brand: e.target.value
                        }))}
                    />
                </div>
            </div>
            <button
                type="submit"
                className={styles.searchButton}
                disabled={filters.brand.trim() === ''}
            >
                Cerca
            </button>
        </form>
    );
}
