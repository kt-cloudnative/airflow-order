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
                , WORKFLOW_NAME='WC_CSNL701D',WORKFLOW_ID='142bf951fc7e44a99d2e0c34e6e879de', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSNL701D-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 16, 20, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('142bf951fc7e44a99d2e0c34e6e879de')

    csnl701dJob_vol = []
    csnl701dJob_volMnt = []
    csnl701dJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnl701dJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csnl701dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnl701dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4a9fff38bce942aaac7060910682c32c',
        'volumes': csnl701dJob_vol,
        'volume_mounts': csnl701dJob_volMnt,
        'env_from':csnl701dJob_env,
        'task_id':'csnl701dJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.5',
        'arguments':["--job.name=csnl701dJob", "lobCd=ET", "testBatchRequestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('142bf951fc7e44a99d2e0c34e6e879de')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnl701dJob,
        Complete
    ]) 

    # authCheck >> csnl701dJob >> Complete
    workflow








