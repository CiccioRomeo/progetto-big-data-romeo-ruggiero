import { ArrowRight } from 'lucide-react';
import type { AssociationRule } from '@/lib/types';
import styles from './rule-card.module.css';

interface RuleCardProps{
    rule: AssociationRule;
}

export function RuleCard({ rule }: RuleCardProps){
    const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;

    return(
        <div className={styles.card}>
            <div className={styles.ruleFlow}>
                <div className={styles.tags}>
                    {rule.antecedent.map(tag => (
                        <span key={tag} className={styles.tag}>{tag}</span>
                    ))}
                </div>
                <ArrowRight className={styles.arrow} />
                <div className={styles.tags}>
                    {rule.consequent.map(tag => (
                        <span key={tag} className={styles.tag}>{tag}</span>
                    ))}
                </div>
            </div>

            <div className={styles.metrics}>
                <div className={styles.metric}>
                    <span className={styles.metricLabel}>Confidenza</span>
                    <span className={styles.metricValue}>{formatPercent(rule.confidence)}</span>
                </div>
                <div className={styles.metric}>
                    <span className={styles.metricLabel}>Supporto</span>
                    <span className={styles.metricValue}>{formatPercent(rule.support)}</span>
                </div>
                <div className={styles.metric}>
                    <span className={styles.metricLabel}>Lift</span>
                    <span className={styles.metricValue}>{rule.lift.toFixed(2)}</span>
                </div>
            </div>
        </div>
    );
}
