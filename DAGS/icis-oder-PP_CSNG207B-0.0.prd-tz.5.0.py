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
                , WORKFLOW_NAME='PP_CSNG207B',WORKFLOW_ID='8b54238a8f7f40c9aee09d2923de91e9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG207B-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 18, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8b54238a8f7f40c9aee09d2923de91e9')

    csng207bJob_vol = []
    csng207bJob_volMnt = []
    csng207bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng207bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng207bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng207bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng207bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng207bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1382e6decf55498b825efddf816c83f4',
        'volumes': csng207bJob_vol,
        'volume_mounts': csng207bJob_volMnt,
        'env_from':csng207bJob_env,
        'task_id':'csng207bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng207bJob", "endTranDate=20241231", "empNo=952794853","ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8b54238a8f7f40c9aee09d2923de91e9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng207bJob,
        Complete
    ]) 

    # authCheck >> csng207bJob >> Complete
    workflow








