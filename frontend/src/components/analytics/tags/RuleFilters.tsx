import { useState } from 'react';
import styles from './rule-filters.module.css';
import { RuleSearchFilters } from '@/lib/types';

interface RuleFiltersProps {
    onSearch: (filters: RuleSearchFilters) => void;
}

export function RuleFilters({ onSearch }: RuleFiltersProps) {
    const [filters, setFilters] = useState<RuleSearchFilters>({
        minSupport: 0.1,
        minConfidence: 0.6,
        tags: [],
    });

    const [newTag, setNewTag] = useState<string>('');

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        onSearch(filters);
    };

    const handleSliderChange = (key: 'minSupport' | 'minConfidence', value: number) => {
        setFilters((prev) => ({
            ...prev,
            [key]: value,
        }));
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
                    {filters.tags.map((tag) => (
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

            <div className={styles.sliders}>
                {/* Slider per minSupport */}
                <div className={styles.controls}>
                    <div className={styles.sliderGroup}>
                        <label htmlFor="minSupport" className={styles.label}>
                            Supporto minimo
                        </label>
                        <input
                            id="minSupport"
                            type="range"
                            min={0.01}
                            max={1}
                            step={0.01}
                            value={filters.minSupport}
                            onChange={(e) => handleSliderChange('minSupport', parseFloat(e.target.value))}
                            className={styles.slider}
                        />
                        <span className={styles.value}>{filters.minSupport.toFixed(2)}</span>
                    </div>
                </div>

                {/* Slider per minConfidence */}
                <div className={styles.controls}>
                    <div className={styles.sliderGroup}>
                        <label htmlFor="minConfidence" className={styles.label}>
                            Confidenza minima
                        </label>
                        <input
                            id="minConfidence"
                            type="range"
                            min={0.1}
                            max={1}
                            step={0.01}
                            value={filters.minConfidence}
                            onChange={(e) => handleSliderChange('minConfidence', parseFloat(e.target.value))}
                            className={styles.slider}
                        />
                        <span className={styles.value}>{filters.minConfidence.toFixed(2)}</span>
                    </div>
                </div>
            </div>

            <button type="submit" className={styles.searchButton}>
                Cerca
            </button>
        </form>
    );
}
