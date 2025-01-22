from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='WC_CBOL550B',WORKFLOW_ID='0f3e0fbbc7e44ecda734fb0531b1e771', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOL550B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 1, 20, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0f3e0fbbc7e44ecda734fb0531b1e771')

    cbol550bJob_vol = []
    cbol550bJob_volMnt = []
    cbol550bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol550bJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    cbol550bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol550bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '66cfe8b2ca3848ca9244448f2f0a8a3e',
        'volumes': cbol550bJob_vol,
        'volume_mounts': cbol550bJob_volMnt,
        'env_from':cbol550bJob_env,
        'task_id':'cbol550bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.4.1.79',
        'arguments':["--job.name=cbol550bJob", "endTranDate=20240723", "testBatchRequestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0f3e0fbbc7e44ecda734fb0531b1e771')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol550bJob,
        Complete
    ]) 

    # authCheck >> cbol550bJob >> Complete
    workflow








