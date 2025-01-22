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
                , WORKFLOW_NAME='WC_CSND222D',WORKFLOW_ID='05404924bd2247a7a1eaaab9a9dc75fe', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSND222D-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 6, 8, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('05404924bd2247a7a1eaaab9a9dc75fe')

    csnd222dJob_vol = []
    csnd222dJob_volMnt = []
    csnd222dJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnd222dJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csnd222dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd222dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4f65a8d0cfa540d789c1d41c2bcb09e4',
        'volumes': csnd222dJob_vol,
        'volume_mounts': csnd222dJob_volMnt,
        'env_from':csnd222dJob_env,
        'task_id':'csnd222dJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.5',
        'arguments':["--job.name=csnd222dJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('05404924bd2247a7a1eaaab9a9dc75fe')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd222dJob,
        Complete
    ]) 

    # authCheck >> csnd222dJob >> Complete
    workflow








