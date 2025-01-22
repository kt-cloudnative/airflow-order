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
                , WORKFLOW_NAME='PP_CSNG197B',WORKFLOW_ID='03458c92e6dc486398b369de3e59afff', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG197B-0.0.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('03458c92e6dc486398b369de3e59afff')

    csng197bJob_vol = []
    csng197bJob_volMnt = []
    csng197bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng197bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng197bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng197bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng197bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng197bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8b9b71854901484b8fcbc671453c0936',
        'volumes': csng197bJob_vol,
        'volume_mounts': csng197bJob_volMnt,
        'env_from':csng197bJob_env,
        'task_id':'csng197bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng197bJob",
"requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('03458c92e6dc486398b369de3e59afff')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng197bJob,
        Complete
    ]) 

    # authCheck >> csng197bJob >> Complete
    workflow








