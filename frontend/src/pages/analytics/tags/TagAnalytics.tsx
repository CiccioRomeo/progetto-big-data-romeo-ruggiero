import { useState, useEffect } from 'react';
import { RuleCard } from '@/components/analytics/tags/RuleCard';
import { RuleFilters } from '@/components/analytics/tags/RuleFilters'; // Importa RuleFilters
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import styles from '@/pages/common.module.css';
import analytics_styles from '../analytics.module.css';
import type { AssociationRule, RuleSearchFilters, TagsResponse } from '@/lib/types';
import { topTags, getAssociationRules } from '@/components/api/api';
import { WordCloudComponent } from '@/components/charts/WordCloud';

export function TagAnalytics() {
    const [rules, setRules] = useState<AssociationRule[]>([]);
    const [tags, setTags] = useState<TagsResponse[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [isSearching, setIsSearching] = useState(false);

    useEffect(() => {
        const fetchData = async () => {
            setIsLoading(true);
            try {
                const tagsData: TagsResponse[] = await topTags(50); 
                setTags(tagsData);
            } catch (error) {
                console.error('Errore nel recupero dei tags:', error);
            } finally {
                setIsLoading(false);
            }
        };
        fetchData();
    }, []);

    const handleSearch = async (filters: RuleSearchFilters) => {
        setIsSearching(true);
        try {
            const associationRules: AssociationRule[] = await getAssociationRules(
                filters.minSupport, 
                filters.minConfidence, 
                filters.tags
            ); 
            setRules(associationRules);
        } catch (error) {
            console.error('Errore nel recupero delle regole:', error);
        } finally {
            setIsSearching(false);
        }
    };

    return (
        <div className={styles.container}>
            <div className={styles.header}>
                <h2 className={styles.title}>Analisi dei tag</h2>
            </div>
            <hr />

            <div className={analytics_styles.explanation}>
                <h1 className={styles.subtitle}>I tag più usati</h1>
                <p>
                    I dati sono visualizzati sotto forma di barplot, che rappresenta il numero di post in cui ogni tag appare.<br />
                    Questo permette di comprendere meglio la popolarità dei diversi tag.
                </p>
            </div>

            <div>
                {isLoading ? (
                    <LoadingSpinner />
                ) : (
                    <WordCloudComponent tags={tags}/>
                )}
            </div>    

            <hr />

            <div>
                <h1 className={styles.subtitle}>Regole di associazione</h1>
                <div className={analytics_styles.explanation}>
                    <p>
                        Queste regole mostrano le relazioni tra i tag presenti nelle foto. Ad esempio, quando determinati tag compaiono insieme,
                        c'è un'elevata probabilità di trovare un altro tag specifico. Le metriche aiutano a comprendere la forza di queste relazioni:
                    </p>
                    <ul className={analytics_styles.metrics}>
                        <li>
                            <strong>Confidenza:</strong> la probabilità che i tag consequenziali compaiano quando i tag antecedenti sono presenti
                        </li>
                        <li>
                            <strong>Supporto:</strong> la frequenza con cui la combinazione appare nel dataset
                        </li>
                        <li>
                            <strong>Lift:</strong> quanto è più probabile che i tag compaiano insieme rispetto al caso
                        </li>
                    </ul>
                </div>

                <RuleFilters
                    onSearch={handleSearch}
                />
                <div className={analytics_styles.results}>
                    {isSearching ? (
                        <div className={analytics_styles.loadingContainer}>
                            <LoadingSpinner />
                        </div>
                    ) : rules.length > 0 ? (
                        <div className={analytics_styles.grid}>
                            {rules.map((rule, index) => (
                                <RuleCard key={index} rule={rule} />
                            ))}
                        </div>
                    ) : (
                        <div className={analytics_styles.noResults}>
                            Non è stata trovata nessuna regola che soddifsi i filtri!
                        </div>
                    )
                    }
                </div>
            </div>
        </div>
    );
}
