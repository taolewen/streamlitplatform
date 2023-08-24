def get_option(channel):
    opdict={
        'Wayfair':{
            'reporttype':['已发货订单','广告','付款','物流发票','CG发货订单'],
            'country':[],
            'area': ['US','EU','CA'],
            'store': ['WF-EU-2','WF-EU-5','WF-US-1','WF-US-2','WF-US-5','WF-CA-1'],
            'week': [],
            'qijian': []

        },
        'Ebay': {
            'reporttype': ['transaction','orders'],
            'country': ['US','DE','UK'],
            'area': ['US','EU'],
            'store': ['EBAY-DE-2','EBAY-UK-2','EBAY-US-2'],
            'week': [],
            'qijian': []
        },
        'CD': {
            'reporttype': ['orderextract','paymentdetail_回款','paymentdetail_结算'],
            'country': ['FR'],
            'area': ['EU'],
            'store': ['CD-1','CD-3','CD-6','CD-HYL'],
            'week': [],
            'qijian': []
        },
        'manomano': {
            'reporttype': ['广告','订单','退款','回款'],
            'country': ['FR','DE'],
            'area': ['EU'],
            'store': ['MM-2','MM-3','MM-3-DE','MM-3-MF'],
            'week': [],
            'qijian': []
        },
        'walmart': {
            'reporttype': ['订单','广告','订单—jd','付款payment','退货','结算(运费)'],
            'country': ['US'],
            'area': ['US'],
            'store': ['WM-JD','WM-2','WM-5','WM-6','WM-7'],
            'week': [],
            'qijian': []
        },
        '独立站': {
            'reporttype': ['订单','广告1_google','广告2_bing'],
            'country': ['US','DE','UK','CA'],
            'area': ['US','EU'],
            'store': ['独立站-DE','独立站-US','独立站-UK','独立站-CA'],
            'week': [],
            'qijian': []
        },

    }
    return opdict[channel]
