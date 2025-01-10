import { useState } from 'react';
import styles from '@/components/users-photos/photo-search/search-filters.module.css';
import { UserSearchFilters } from '@/lib/types';

interface SearchFiltersProps{
    onSearch: (filters: UserSearchFilters) => void;
}

export function OwnerSearchFilters({ onSearch }: SearchFiltersProps ){

    const [filters, setFilters] = useState<UserSearchFilters>({
        owner: ''
    });
    
    const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSearch(filters);
    };
    
    return (
        <form onSubmit={handleSubmit} className={styles.form}>
            <div className={styles.textFilters}>
                <div className={styles.input}>
                    <label htmlFor='username'>Nome utente</label>
                    <input 
                        type='text' 
                        id='owner' 
                        placeholder='Cerca...' 
                        value={filters.owner}
                        onChange={e => setFilters(prev => ({
                            ...prev,
                            owner: e.target.value
                        }))}
                    />
                </div>
            </div>
            <button
                type="submit"
                className={styles.searchButton}
                disabled={filters.owner.trim() === ''}
            >
                Cerca
            </button>
        </form>
    );
}
