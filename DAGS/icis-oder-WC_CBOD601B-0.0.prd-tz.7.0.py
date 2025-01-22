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
                , WORKFLOW_NAME='WC_CBOD601B',WORKFLOW_ID='e9a186aab15b4aefbdd8a27a51e52483', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOD601B-0.0.prd-tz.7.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e9a186aab15b4aefbdd8a27a51e52483')

    cbod601bJob_vol = []
    cbod601bJob_volMnt = []
    cbod601bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbod601bJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    cbod601bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod601bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1565dcdb450c4eb5b54174a265069116',
        'volumes': cbod601bJob_vol,
        'volume_mounts': cbod601bJob_volMnt,
        'env_from':cbod601bJob_env,
        'task_id':'cbod601bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.7',
        'arguments':["--job.name=cbod601bJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e9a186aab15b4aefbdd8a27a51e52483')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod601bJob,
        Complete
    ]) 

    # authCheck >> cbod601bJob >> Complete
    workflow








