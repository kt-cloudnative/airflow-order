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
                , WORKFLOW_NAME='hist010b_report',WORKFLOW_ID='238ec0a6966f4130a2310385e8828a68', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-hist010b_report-0.1.prd-tz.0.0'
    ,'schedule_interval':'30 6 * * *'
    ,'start_date': datetime(2024, 12, 11, 0, 2, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('238ec0a6966f4130a2310385e8828a68')

    hist010b_vol = []
    hist010b_volMnt = []
    hist010b_vol.append(getVolume('shared-volume','shared-volume'))
    hist010b_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    hist010b_env = [getICISConfigMap('icis-oder-batch-sample-test-configmap'), getICISConfigMap('icis-oder-batch-sample-test-configmap2'), getICISSecret('icis-oder-batch-sample-test-secret')]
    hist010b_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    hist010b_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    hist010b = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2df885ad0fa64532862341daa2f5037c',
        'volumes': hist010b_vol,
        'volume_mounts': hist010b_volMnt,
        'env_from':hist010b_env,
        'task_id':'hist010b',
        'image':'/icis/icis-oder-batch-sample-test:0.7.1.3',
        'arguments':["--job.name=hist010bJob", "ignoredJobs=hist010b-redis", "workEnv=PRD"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('238ec0a6966f4130a2310385e8828a68')

    workflow = COMMON.getICISPipeline([
        authCheck,
        hist010b,
        Complete
    ]) 

    # authCheck >> hist010b >> Complete
    workflow








