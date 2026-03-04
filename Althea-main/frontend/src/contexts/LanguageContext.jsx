import { createContext, useContext, useEffect, useMemo, useState } from 'react'

const DEFAULT_LANGUAGE = 'en'

const SUPPORTED_LANGUAGES = [
  { code: 'en', label: 'English' },
  { code: 'ru', label: 'Русский' },
  { code: 'zh', label: '中文' },
  { code: 'ja', label: '日本語' },
  { code: 'tr', label: 'Türkçe' },
  { code: 'kk', label: 'Қазақша' },
]

const messages = {
  en: {
    layout: {
      pageTitles: {
        '/': 'Home',
        '/alert-queue': 'Alert Queue',
        '/cases': 'Cases',
        '/ops': 'Operations & Governance',
        '/data': 'System & Data',
      },
      defaultTitle: 'Althea',
      languageLabel: 'Language',
    },
    sidebar: {
      navigation: 'NAVIGATION',
      analyst: 'ANALYST',
      collapse: 'Collapse navigation',
      expand: 'Expand navigation',
      nav: {
        '/': 'Home',
        '/alert-queue': 'Alert Queue',
        '/cases': 'Cases',
        '/ops': 'Ops & Governance',
        '/data': 'System & Data',
      },
    },
    home: {
      heroTag: 'Behavioral AML Platform',
      heroTitleTop: 'Behavioral',
      heroTitleBottom: 'AML Governance',
      heroText:
        'We combine behavioral analytics and decision governance so teams can surface truly critical signals faster.',
      startDemo: 'Start Demo',
      viewCases: 'View Cases',
      mediaAria: 'Placeholder for analytics video',
      mediaTag: 'Video / Image Placeholder',
      mediaText: 'A scene for behavioral analysis, event analytics, and risk monitoring.',
      capabilitiesTitle: 'Core Capabilities',
      capabilitiesAria: 'Platform core capabilities',
      capabilities: [
        {
          title: 'Behavioral Analysis',
          desc: 'Personalized behavior modeling with early anomaly detection.',
          icon: '◎',
        },
        {
          title: 'Risk Calibration',
          desc: 'Unified 0-100 scale with explainable ranges and clear logic.',
          icon: '◍',
        },
        {
          title: 'Governance Engine',
          desc: 'Priority-aware alert routing based on available team capacity.',
          icon: '▦',
        },
        {
          title: 'Audit & Transparency',
          desc: 'Versioned rules, decision reasons, and immutable event logs.',
          icon: '◬',
        },
      ],
      impactTitle: 'Why It Matters',
      impactAria: 'Why this matters',
      impactBullets: [
        'Reduce analyst load through dynamic prioritization.',
        'Increase decision quality at fixed operating capacity.',
        'Strengthen regulatory defensibility for each decision cycle.',
        'Prevent critical-alert backlog accumulation.',
      ],
      missionTitle: 'Our Mission',
      missionTextOne:
        'We build world-class AML governance where strong engineering, practical analytics, and risk-ops expertise deliver stable decision quality.',
      missionTextTwo:
        'Our focus is safe innovation: explainable models, transparent analytics, and architecture ready for regulatory scrutiny.',
      missionAria: 'Placeholder for team photo or chart',
      missionMedia: 'Team photo / Performance chart',
      ctaTitle: 'Ready for governed AML scale?',
      ctaText:
        'See how Althea reduces noise, protects SLA, and improves explainability for every decision.',
      tryService: 'Try Service',
      footerFaq: 'FAQ',
      footerReviews: 'Reviews',
      footerPolicy: 'Security Policy',
    },
  },
  ru: {
    layout: {
      pageTitles: {
        '/': 'Главная',
        '/alert-queue': 'Очередь алертов',
        '/cases': 'Кейсы',
        '/ops': 'Операции и контроль',
        '/data': 'Система и данные',
      },
      defaultTitle: 'Althea',
      languageLabel: 'Язык',
    },
    sidebar: {
      navigation: 'НАВИГАЦИЯ',
      analyst: 'АНАЛИТИК',
      collapse: 'Свернуть навигацию',
      expand: 'Развернуть навигацию',
      nav: {
        '/': 'Главная',
        '/alert-queue': 'Очередь алертов',
        '/cases': 'Кейсы',
        '/ops': 'Операции и контроль',
        '/data': 'Система и данные',
      },
    },
    home: {
      heroTag: 'Платформа Behavioral AML',
      heroTitleTop: 'Поведенческий',
      heroTitleBottom: 'AML контроль',
      heroText:
        'Мы объединяем поведенческую аналитику и управляемое принятие решений, чтобы команды быстрее находили действительно критичные сигналы.',
      startDemo: 'Запустить демо',
      viewCases: 'Посмотреть кейсы',
      mediaAria: 'Плейсхолдер для аналитического видео',
      mediaTag: 'Плейсхолдер видео / изображения',
      mediaText: 'Сцена для поведенческого анализа, событийной аналитики и мониторинга риска.',
      capabilitiesTitle: 'Ключевые возможности',
      capabilitiesAria: 'Ключевые возможности платформы',
      capabilities: [
        {
          title: 'Поведенческий анализ',
          desc: 'Персонифицированное моделирование поведения и раннее определение аномалий.',
          icon: '◎',
        },
        {
          title: 'Калибровка риска',
          desc: 'Единая шкала 0-100 с объяснимыми диапазонами и прозрачной логикой.',
          icon: '◍',
        },
        {
          title: 'Движок управления',
          desc: 'Распределение алертов по приоритету и доступным ресурсам команды.',
          icon: '▦',
        },
        {
          title: 'Аудит и прозрачность',
          desc: 'Версии правил, причины решений и неизменяемые журналы событий.',
          icon: '◬',
        },
      ],
      impactTitle: 'Почему это важно',
      impactAria: 'Почему это важно',
      impactBullets: [
        'Снижение нагрузки на аналитиков за счет динамической приоритизации.',
        'Рост точности решений при фиксированной операционной мощности.',
        'Более сильная регуляторная защитимость каждой итерации.',
        'Предотвращение накопления критических алертов в очереди.',
      ],
      missionTitle: 'Наша миссия',
      missionTextOne:
        'Мы создаем world-class AML governance, где сильная инженерия, прикладная аналитика и опыт risk-операций дают командам устойчивое качество решений.',
      missionTextTwo:
        'Наш фокус - безопасные инновации: объяснимые алгоритмы, прозрачная аналитика и архитектура, готовая к регуляторному контролю.',
      missionAria: 'Плейсхолдер для фото команды или графика',
      missionMedia: 'Фото команды / График эффективности',
      ctaTitle: 'Готовы к управляемому AML-масштабу?',
      ctaText:
        'Покажем, как Althea снижает шум, защищает SLA и повышает объяснимость каждого решения.',
      tryService: 'Попробовать сервис',
      footerFaq: 'FAQ',
      footerReviews: 'Отзывы',
      footerPolicy: 'Политика безопасности',
    },
  },
  zh: {
    layout: {
      pageTitles: {
        '/': '首页',
        '/alert-queue': '告警队列',
        '/cases': '案件',
        '/ops': '运营与治理',
        '/data': '系统与数据',
      },
      defaultTitle: 'Althea',
      languageLabel: '语言',
    },
    sidebar: {
      navigation: '导航',
      analyst: '分析师',
      collapse: '折叠导航',
      expand: '展开导航',
      nav: {
        '/': '首页',
        '/alert-queue': '告警队列',
        '/cases': '案件',
        '/ops': '运营与治理',
        '/data': '系统与数据',
      },
    },
    home: {
      heroTag: '行为 AML 平台',
      heroTitleTop: '行为化',
      heroTitleBottom: 'AML 治理',
      heroText: '我们将行为分析与决策治理结合，帮助团队更快定位真正关键的风险信号。',
      startDemo: '开始演示',
      viewCases: '查看案例',
      mediaAria: '分析视频占位区',
      mediaTag: '视频 / 图片占位',
      mediaText: '用于行为分析、事件分析与风险监控的展示区域。',
      capabilitiesTitle: '核心能力',
      capabilitiesAria: '平台核心能力',
      capabilities: [
        { title: '行为分析', desc: '个性化行为建模与异常早期识别。', icon: '◎' },
        { title: '风险校准', desc: '统一 0-100 评分并提供可解释区间。', icon: '◍' },
        { title: '治理引擎', desc: '基于优先级和团队容量进行告警分配。', icon: '▦' },
        { title: '审计与透明', desc: '规则版本、决策原因与不可篡改日志。', icon: '◬' },
      ],
      impactTitle: '为何重要',
      impactAria: '为何重要',
      impactBullets: [
        '通过动态优先级降低分析师负荷。',
        '在固定产能下提升决策质量。',
        '增强每轮决策的监管可辩护性。',
        '避免关键告警积压。',
      ],
      missionTitle: '我们的使命',
      missionTextOne: '我们打造世界级 AML 治理，将工程能力、实战分析与风控运营经验结合起来。',
      missionTextTwo: '我们专注于安全创新：可解释模型、透明分析以及可接受监管审查的架构。',
      missionAria: '团队照片或图表占位',
      missionMedia: '团队照片 / 性能图表',
      ctaTitle: '准备好实现可治理的 AML 规模化了吗？',
      ctaText: '了解 Althea 如何降低噪声、保障 SLA，并提升每个决策的可解释性。',
      tryService: '立即体验',
      footerFaq: '常见问题',
      footerReviews: '客户评价',
      footerPolicy: '安全政策',
    },
  },
  ja: {
    layout: {
      pageTitles: {
        '/': 'ホーム',
        '/alert-queue': 'アラートキュー',
        '/cases': 'ケース',
        '/ops': '運用とガバナンス',
        '/data': 'システムとデータ',
      },
      defaultTitle: 'Althea',
      languageLabel: '言語',
    },
    sidebar: {
      navigation: 'ナビゲーション',
      analyst: 'アナリスト',
      collapse: 'ナビゲーションを折りたたむ',
      expand: 'ナビゲーションを展開',
      nav: {
        '/': 'ホーム',
        '/alert-queue': 'アラートキュー',
        '/cases': 'ケース',
        '/ops': '運用とガバナンス',
        '/data': 'システムとデータ',
      },
    },
    home: {
      heroTag: 'Behavioral AML プラットフォーム',
      heroTitleTop: 'Behavioral',
      heroTitleBottom: 'AML Governance',
      heroText:
        '行動分析と意思決定ガバナンスを組み合わせ、チームが本当に重要なシグナルをより早く見つけられるようにします。',
      startDemo: 'デモを開始',
      viewCases: 'ケースを見る',
      mediaAria: '分析動画プレースホルダー',
      mediaTag: '動画 / 画像プレースホルダー',
      mediaText: '行動分析、イベント分析、リスク監視のための表示領域。',
      capabilitiesTitle: '主要機能',
      capabilitiesAria: 'プラットフォーム主要機能',
      capabilities: [
        { title: '行動分析', desc: '個別行動モデリングと早期異常検知。', icon: '◎' },
        { title: 'リスク校正', desc: '説明可能な 0-100 の統一スコア。', icon: '◍' },
        { title: 'ガバナンスエンジン', desc: '優先度と稼働余力に応じたアラート配分。', icon: '▦' },
        { title: '監査と透明性', desc: 'ルール版管理、判断根拠、不変ログ。', icon: '◬' },
      ],
      impactTitle: 'なぜ重要か',
      impactAria: 'なぜ重要か',
      impactBullets: [
        '動的優先度付けでアナリスト負荷を軽減。',
        '固定キャパシティでも判断精度を向上。',
        '各サイクルの規制対応力を強化。',
        '重要アラートの滞留を防止。',
      ],
      missionTitle: '私たちのミッション',
      missionTextOne:
        '強いエンジニアリング、実践的分析、リスク運用知見を組み合わせ、世界水準の AML ガバナンスを実現します。',
      missionTextTwo:
        '私たちは安全なイノベーションに注力します。説明可能なモデル、透明な分析、規制監査に耐えるアーキテクチャです。',
      missionAria: 'チーム写真またはチャートのプレースホルダー',
      missionMedia: 'チーム写真 / パフォーマンスチャート',
      ctaTitle: 'ガバナンス可能な AML スケールへ進みますか？',
      ctaText: 'Althea がノイズを減らし、SLA を守り、判断の説明可能性を高める方法をご紹介します。',
      tryService: 'サービスを試す',
      footerFaq: 'FAQ',
      footerReviews: 'レビュー',
      footerPolicy: 'セキュリティポリシー',
    },
  },
  tr: {
    layout: {
      pageTitles: {
        '/': 'Ana Sayfa',
        '/alert-queue': 'Uyari Kuyrugu',
        '/cases': 'Vakalar',
        '/ops': 'Operasyon ve Yonetisim',
        '/data': 'Sistem ve Veri',
      },
      defaultTitle: 'Althea',
      languageLabel: 'Dil',
    },
    sidebar: {
      navigation: 'GEZINME',
      analyst: 'ANALIST',
      collapse: 'Gezinmeyi daralt',
      expand: 'Gezinmeyi genislet',
      nav: {
        '/': 'Ana Sayfa',
        '/alert-queue': 'Uyari Kuyrugu',
        '/cases': 'Vakalar',
        '/ops': 'Operasyon ve Yonetisim',
        '/data': 'Sistem ve Veri',
      },
    },
    home: {
      heroTag: 'Behavioral AML Platformu',
      heroTitleTop: 'Behavioral',
      heroTitleBottom: 'AML Governance',
      heroText:
        'Ekiplerin gercekten kritik sinyalleri daha hizli bulmasi icin davranissal analitik ile karar yonetisimini birlestiriyoruz.',
      startDemo: 'Demoyu Baslat',
      viewCases: 'Vakalari Gor',
      mediaAria: 'Analitik video yer tutucu',
      mediaTag: 'Video / Gorsel Yer Tutucu',
      mediaText: 'Davranissal analiz, olay analitigi ve risk izleme icin sahne.',
      capabilitiesTitle: 'Temel Yetenekler',
      capabilitiesAria: 'Platform temel yetenekleri',
      capabilities: [
        { title: 'Davranissal Analiz', desc: 'Kisisellestirilmis modelleme ve erken sapma tespiti.', icon: '◎' },
        { title: 'Risk Kalibrasyonu', desc: 'Aciklanabilir araliklara sahip birlesik 0-100 skala.', icon: '◍' },
        { title: 'Yonetisim Motoru', desc: 'Oncelik ve ekip kapasitesine gore uyari dagitimi.', icon: '▦' },
        { title: 'Denetim ve Seffaflik', desc: 'Kural surumleri, karar nedenleri ve degistirilemez loglar.', icon: '◬' },
      ],
      impactTitle: 'Neden Onemli',
      impactAria: 'Neden onemli',
      impactBullets: [
        'Dinamik onceliklendirme ile analist yukunu azaltir.',
        'Sabit kapasitede karar kalitesini artirir.',
        'Her dongude duzenleyici savunulabilirligi guclendirir.',
        'Kritik uyari birikimini onler.',
      ],
      missionTitle: 'Misyonumuz',
      missionTextOne:
        'Guclu muhendislik, uygulamali analitik ve risk operasyonu deneyimi ile dunya standartlarinda AML yonetisimi kuruyoruz.',
      missionTextTwo:
        'Odak noktamiz guvenli inovasyon: aciklanabilir modeller, seffaf analitik ve denetime hazir mimari.',
      missionAria: 'Ekip fotografi veya grafik yer tutucu',
      missionMedia: 'Ekip Fotografi / Performans Grafigi',
      ctaTitle: 'Yonetilebilir AML olcegine hazir misiniz?',
      ctaText: 'Althea\'nin gurultuyu nasil azalttigini, SLA\'yi korudugunu ve aciklanabilirligi artirdigini gorun.',
      tryService: 'Servisi Dene',
      footerFaq: 'SSS',
      footerReviews: 'Yorumlar',
      footerPolicy: 'Guvenlik Politikasi',
    },
  },
  kk: {
    layout: {
      pageTitles: {
        '/': 'Басты бет',
        '/alert-queue': 'Ескертулер кезегі',
        '/cases': 'Кейстер',
        '/ops': 'Операциялар және бақылау',
        '/data': 'Жүйе және деректер',
      },
      defaultTitle: 'Althea',
      languageLabel: 'Тіл',
    },
    sidebar: {
      navigation: 'НАВИГАЦИЯ',
      analyst: 'ТАЛДАУШЫ',
      collapse: 'Навигацияны жинау',
      expand: 'Навигацияны ашу',
      nav: {
        '/': 'Басты бет',
        '/alert-queue': 'Ескертулер кезегі',
        '/cases': 'Кейстер',
        '/ops': 'Операциялар және бақылау',
        '/data': 'Жүйе және деректер',
      },
    },
    home: {
      heroTag: 'Behavioral AML платформасы',
      heroTitleTop: 'Behavioral',
      heroTitleBottom: 'AML Governance',
      heroText:
        'Топтар шынымен маңызды сигналдарды тез табуы үшін мінез-құлық аналитикасы мен шешім басқаруын біріктіреміз.',
      startDemo: 'Демоны бастау',
      viewCases: 'Кейстерді қарау',
      mediaAria: 'Аналитикалық видеоға орын',
      mediaTag: 'Видео / Сурет орны',
      mediaText: 'Мінез-құлық талдауы, оқиға аналитикасы және тәуекел мониторингі үшін аймақ.',
      capabilitiesTitle: 'Негізгі мүмкіндіктер',
      capabilitiesAria: 'Платформаның негізгі мүмкіндіктері',
      capabilities: [
        { title: 'Мінез-құлық талдауы', desc: 'Жеке модельдеу және ауытқуды ерте анықтау.', icon: '◎' },
        { title: 'Тәуекелді калибрлеу', desc: 'Түсіндірілетін диапазондармен бірыңғай 0-100 шкаласы.', icon: '◍' },
        { title: 'Басқару қозғалтқышы', desc: 'Басымдық пен команда қуатына сай ескертулерді бөлу.', icon: '▦' },
        { title: 'Аудит және ашықтық', desc: 'Ереже нұсқалары, шешім себептері және өзгермейтін логтар.', icon: '◬' },
      ],
      impactTitle: 'Неліктен маңызды',
      impactAria: 'Неліктен маңызды',
      impactBullets: [
        'Динамикалық басымдық арқылы талдаушы жүктемесін азайтады.',
        'Бірдей қуатта шешім сапасын арттырады.',
        'Әр циклдің реттеушілік қорғалуын күшейтеді.',
        'Маңызды ескертулердің жиналуын болдырмайды.',
      ],
      missionTitle: 'Біздің миссия',
      missionTextOne:
        'Күшті инженерия, қолданбалы аналитика және risk-операция тәжірибесін біріктіріп, әлемдік деңгейдегі AML басқаруын жасаймыз.',
      missionTextTwo:
        'Біздің фокус - қауіпсіз инновация: түсіндірілетін модельдер, ашық аналитика және реттеуші тексеруге дайын архитектура.',
      missionAria: 'Команда фотосы не график орны',
      missionMedia: 'Команда фотосы / Өнімділік графигі',
      ctaTitle: 'Басқарылатын AML ауқымына дайынсыз ба?',
      ctaText:
        'Althea шуды қалай азайтатынын, SLA-ны қорғайтынын және әр шешімнің түсіндірмесін күшейтетінін көрсетеміз.',
      tryService: 'Қызметті көру',
      footerFaq: 'FAQ',
      footerReviews: 'Пікірлер',
      footerPolicy: 'Қауіпсіздік саясаты',
    },
  },
}

const LanguageContext = createContext()

export function LanguageProvider({ children }) {
  const [language, setLanguage] = useState(() => {
    const stored = localStorage.getItem('language')
    return messages[stored] ? stored : DEFAULT_LANGUAGE
  })

  useEffect(() => {
    if (!messages[language]) {
      setLanguage(DEFAULT_LANGUAGE)
      return
    }
    localStorage.setItem('language', language)
    document.documentElement.setAttribute('lang', language)
  }, [language])

  const value = useMemo(
    () => ({
      language,
      setLanguage,
      languages: SUPPORTED_LANGUAGES,
      t: messages[language] ?? messages[DEFAULT_LANGUAGE],
    }),
    [language]
  )

  return <LanguageContext.Provider value={value}>{children}</LanguageContext.Provider>
}

export const useLanguage = () => useContext(LanguageContext)
