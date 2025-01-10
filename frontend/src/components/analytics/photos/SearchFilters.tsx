import { useState } from 'react';
import styles from './search-filters.module.css';
import { PhotoSearchFilters } from '@/lib/types';

interface SearchFiltersProps {
    onSearch: (filters: PhotoSearchFilters) => void;
}

export function SearchFilters({ onSearch }: SearchFiltersProps) {
    const MIN_DATE = '2000-01-01';
    const MAX_DATE = '2017-12-31';

    const [filters, setFilters] = useState<PhotoSearchFilters>({
        startDate: MIN_DATE,
        endDate: MAX_DATE,
        keyword: '',
        tags: [],
    });

    const [newTag, setNewTag] = useState<string>('');

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        onSearch(filters);
    };

    const handleAddTag = () => {
        if (newTag.trim() && !filters.tags.includes(newTag)) {
            setFilters(prev => ({
                ...prev,
                tags: [...prev.tags, newTag.trim()],
            }));
            setNewTag(''); // Resetta il campo input
        }
    };

    const handleRemoveTag = (tagToRemove: string) => {
        setFilters(prev => ({
            ...prev,
            tags: prev.tags.filter(tag => tag !== tagToRemove),
        }));
    };

    return (
        <form onSubmit={handleSubmit} className={styles.form}>
            <div className={styles.textFilters}>
                <div className={styles.input}>
                    <label htmlFor="keyword">Keyword</label>
                    <input
                        type="text"
                        id="keyword"
                        placeholder="Cerca per keyword..."
                        value={filters.keyword}
                        onChange={e =>
                            setFilters(prev => ({
                                ...prev,
                                keyword: e.target.value,
                            }))
                        }
                    />
                </div>
            </div>
            <div className={styles.tagSection}>
                <div className={styles.input}>
                    <label htmlFor="newTag">Tags</label>
                        <div className={styles.tagInputWrapper}>
                            <button
                                type="button"
                                onClick={handleAddTag}
                                className={styles.addTagButton}
                            >
                                +
                            </button>
                            <input
                                type="text"
                                id="newTag"
                                placeholder="Aggiungi un tag..."
                                value={newTag}
                                onChange={e => setNewTag(e.target.value)}
                            />
                        </div>
                </div>
                <div className={styles.selectedTags}>
                    {filters.tags.map(tag => (
                        <span key={tag} className={styles.tag}>
                            {tag}
                            <button
                                type="button"
                                onClick={() => handleRemoveTag(tag)}
                                className={styles.removeTag}
                            >
                                ×
                            </button>
                        </span>
                    ))}
                </div>
            </div>
            <div className={styles.dateFilters}>
                <div className={styles.datePicker}>
                    <label htmlFor="startDate">Data Inizio</label>
                    <input
                        type="date"
                        id="startDate"
                        value={filters.startDate}
                        min={MIN_DATE}
                        max={MAX_DATE}
                        onChange={e =>
                            setFilters(prev => ({
                                ...prev,
                                startDate: e.target.value,
                                endDate:
                                    prev.endDate && prev.endDate < e.target.value
                                        ? ''
                                        : prev.endDate,
                            }))
                        }
                    />
                </div>
                <div className={styles.datePicker}>
                    <label htmlFor="endDate">Data Fine</label>
                    <input
                        type="date"
                        id="endDate"
                        value={filters.endDate}
                        min={filters.startDate || MIN_DATE}
                        max={MAX_DATE}
                        onChange={e =>
                            setFilters(prev => ({
                                ...prev,
                                endDate: e.target.value,
                            }))
                        }
                    />
                </div>
            </div>
            <button
                type="submit"
                className={styles.searchButton}
                disabled={filters.tags.length === 0 && filters.keyword.trim() === ''}
            >
                Cerca
            </button>
        </form>
    );
}
