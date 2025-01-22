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
                , WORKFLOW_NAME='WC_CSND223D',WORKFLOW_ID='8c10989b151d4a9d991854f13ed55444', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSND223D-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 6, 8, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8c10989b151d4a9d991854f13ed55444')

    csnd223dJob_vol = []
    csnd223dJob_volMnt = []
    csnd223dJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnd223dJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csnd223dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd223dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4a52a78a94bf4b84b7adcfbed11dd988',
        'volumes': csnd223dJob_vol,
        'volume_mounts': csnd223dJob_volMnt,
        'env_from':csnd223dJob_env,
        'task_id':'csnd223dJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.5',
        'arguments':["--job.name=csnd223dJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8c10989b151d4a9d991854f13ed55444')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd223dJob,
        Complete
    ]) 

    # authCheck >> csnd223dJob >> Complete
    workflow








