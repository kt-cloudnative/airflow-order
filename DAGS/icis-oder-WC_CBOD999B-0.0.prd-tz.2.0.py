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
                , WORKFLOW_NAME='WC_CBOD999B',WORKFLOW_ID='0d51e437e16c43599d5188f7d7ae4833', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOD999B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0d51e437e16c43599d5188f7d7ae4833')

    cbod999bJob_vol = []
    cbod999bJob_volMnt = []
    cbod999bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod999bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod999bJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    cbod999bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod999bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod999bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '549745e6e2b546a8a594682e36ed6e64',
        'volumes': cbod999bJob_vol,
        'volume_mounts': cbod999bJob_volMnt,
        'env_from':cbod999bJob_env,
        'task_id':'cbod999bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.2',
        'arguments':["--job.name=cbod999bJob", "pgmNm=cbod999b", "endTranDate=20240731", "empNo=091015557", "workDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0d51e437e16c43599d5188f7d7ae4833')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod999bJob,
        Complete
    ]) 

    # authCheck >> cbod999bJob >> Complete
    workflow








